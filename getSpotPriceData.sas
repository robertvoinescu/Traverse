%macro getSpotPriceData(SpotPriceIdList=1 3 1000462,
						OutputTable=SpotPriceDataTable,
					    expandInt=hour1,
					    dataStartDate=,
						dataEndDate=);

proc sql;
	create table _spotPriceIdTable as
	select s.*
	from powersim.spotPriceIdTable s
	where s.SpotPriceId in (&SpotPriceIdList.)
    order by s.SpotPriceId;
quit;

%SysErrReturn;

%let maxRowN=0;

data _spotPriceIdTable;
	set _spotPriceIdTable;
	rowN=_n_;
	call symput('maxRowN',rowN);
run;

%SysErrReturn;

proc sql;
	drop table &OutputTable.;
quit;

%if &maxRowN. gt 0 %then %do currentRow=1 %to &maxRowN.;

data _currentSpotPriceIdTable;
	set _spotPriceIdTable(where=(rowN=&currentRow.));
	if compress(LookupIdValues) eq "" then LookupIdValues=SpotPriceId;
	call symput('currentSpotPriceId',trim(left(SpotPriceId)));
	call symput('LookupIdValues',trim(left(LookupIdValues)));
	call symput('InputTableMapId',compress(InputTableMapIdTable));
run;

%SysErrReturn;

%GetInputTableData(InputTableMapId=&InputTableMapId.,
	                     InputTableIdDescription=SpotPriceDataTable,
						 LookupIdList=&LookupIdValues.,
						 outputTable=_OutputTable&currentSpotPriceId.,
					     dataStartDate=&dataStartDate.,
						 dataEndDate=&dataEndDate.,
						 SpotPriceId=&currentSpotPriceId.);



/* find date for most current data */
proc sql;
	select max(&DATVARIABLE.) into :MostCurrentDate format=date9.
	from &outputTable.
quit;

/* fill in the */
%if %sysfunc(compress("&dataEndDate.")) ne "" and &MostCurrentDate le &dataEndDate. %then %do;
	%TraverseRequest(StartDate=&MostCurrentDate., EndDate=&dataEndDate.,SpotPriceId=&currentSpotPriceId.,OutputTable=AddAPIDataTable);
%end;

data AddSpotPriceDataTable;
	_OutputTable&currentSpotPriceId.
	set powersim.spotPriceDataTable(where=(spotPriceId=&currentSpotPriceId. and price ne .));
run;

data _OutputTable&currentSpotPriceId.;
	set _OutputTable&currentSpotPriceId.(where=(price ne .)) /* JRF - 2/10/21 - Anand and I decided it makes more sense to just use the specified source - AddSpotPriceDataTable */;
	spotPriceId=&currentSpotPriceId.;
	if updateDateTime gt &scheduledRunDateTimeNum. then delete;

	/*

	if endDate gt intnx('dtday',&scheduledRunDateTimeNum.,0,'e') then delete;

	*/

	%if %symexist(SpotPriceEndDateTime) %then %do;

		if endDate gt intnx('dtday',&SpotPriceEndDateTime.,0,'e') then delete;

	%end;

	/* 2012-11-09 SN: filter out bad data */
	if startdate=. or endDate=. then delete;



	%if %symexist(GetMinSpotHistDate) %then %do;

		if startDate lt &GetMinSpotHistDate. then delete;

	%end;

	/* SNB added on Nov 25 2017 to take care of end of day prices  */

	if hour(endDate)=23 and minute(endDate)=59 then endDate=intnx('hour',endDate,1,'b');
	
run;

%SysErrReturn;

proc sort data=_OutputTable&currentSpotPriceId.;
	by spotPriceId /* startDate */ endDate updateDateTime price;
run;

%SysErrReturn;

data _OutputTable&currentSpotPriceId.;
	set _OutputTable&currentSpotPriceId.;
	by spotPriceId /* startDate */ endDate;

	if startDate=endDate then delete;

	if last.endDate then output;
	drop updateDateTime;
run;

%SysErrReturn;

proc sort data=_OutputTable&currentSpotPriceId. nodupkey;
	by spotPriceId startDate endDate;
run;

%SysErrReturn;

data _OutputTable&currentSpotPriceId.;
	set _OutputTable&currentSpotPriceId.;
	rowN=_n_;
run;

%SysErrReturn;

data _OutputTable2x&currentSpotPriceId.;
	set _OutputTable&currentSpotPriceId.;
	eventDateTime=intnx("&expandInt.", startDate, 0, 'b');
	output;
	eventDateTime=intnx("&expandInt.", EndDate-1, 0, 'e')+1;
	output;
run;

%SysErrReturn;

%let OutputTableCount=0;

proc sql noprint;
	select count(*) 
	into :OutputTableCount
	from _OutputTable2x&currentSpotPriceId. ;
quit;

%if &OutputTableCount > 0 %then %do;

	proc sort data=_OutputTable2x&currentSpotPriceId. nodupkey;
		by SpotPriceId rowN eventDateTime; 
	run;

	proc expand data=_OutputTable2x&currentSpotPriceId. out=_OutputTable2xEXP&currentSpotPriceId. to=&expandInt. method=step;
		by SpotPriceId rowN;
		id eventDateTime;
		var price;
	run;
	quit;

%SysErrReturn;

data _SpotPriceDataTable&currentSpotPriceId.;
	set _OutputTable2xEXP&currentSpotPriceId.;
	by SpotPriceId rowN;
	if first.rowN then delete;
	drop rowN;
run;

%SysErrReturn;

proc means noprint nway data=_SpotPriceDataTable&currentSpotPriceId.;
     class SpotPriceId eventDateTime;
	 var price;
	 output out=_hourlyPrices&currentSpotPriceId.(drop=_type_ _freq_) mean=;
run;

%SysErrReturn;

proc append base=&OutputTable. data=_hourlyPrices&currentSpotPriceId. force;
run;

%end;

%else %do;
	
	data _hourlyPrices&currentSpotPriceId.;
	    spotpriceid=.;
		eventDateTime=.;
		price=.;

		/*

		if eventDateTime gt intnx('dtday',&scheduledRunDateTimeNum.,0,'e') then delete;

		*/
		format eventDateTime datetime.;
	run;


proc append base=&OutputTable. data=_hourlyPrices&currentSpotPriceId.(obs=0) force;
run;


%end;

/* VK 2018-08-07 this section makes everything hourly.  Need to make it conditional on timestep actually being hourly */
data _null_;
	TakeHourlyMean = index(upcase("&expandInt."), 'HOUR');
	call symputx('TakeHourlyMean', TakeHourlyMean);
run;

%if %sysevalf(&TakeHourlyMean. gt 0) %then %do;

data &outputTable.;
	set &outputTable.;
	hour = intnx('hour', eventDateTime, 0, 'b');
run;

proc means data=&outputTable. noprint nway;
	class hour spotpriceid;
	output out=_hourlyOutput mean(price)=price;
run;

data &outputTable.;
	set _hourlyOutput;
	eventDateTime = hour;
	keep eventDateTime spotPriceId price;
run;

%end;


%SysErrReturn;

proc sql;
	drop table _OutputTable&currentSpotPriceId.;
	drop table _OutputTable2x&currentSpotPriceId.;
	drop table _OutputTable2xEXP&currentSpotPriceId.;
	drop table _SpotPriceDataTable&currentSpotPriceId.;
	drop table _hourlyPrices&currentSpotPriceId.;
quit;

%end;

proc sql;
	drop table _currentSpotPriceIdTable;
    drop table _spotPriceIdTable;
quit;

%if %sysfunc(exist(&OutputTable.))=0 %then %do;

data &OutputTable.;
	SpotPriceId=.;
	eventDateTime=.;
	price=.;
run;


data &OutputTable.;
	set &outputTable(obs=0);
run;

%end;

%if %symexist(SpotPriceNoise) %then %do;

	data &OutputTable.;
		set &OutputTable.;
		price=price * (1 + rannor(1234) * &SpotPriceNoise.);
	run;

%end;

%mend getSpotPriceData;
