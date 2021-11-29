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

data AddSpotPriceDataTable;
	set powersim.spotPriceDataTable(where=(spotPriceId=&currentSpotPriceId. and price ne . and startdate ne .
		and enddate ne .));
run;

%if %symexist(ReloadSpotPriceData)=0 %then %do;
	%global ReloadSpotPriceData;
	%let ReloadSpotPriceData=1;
%end;

%if %sysevalf(&ReloadSpotPriceData. ne 0) %then %do;

	%let maxPSSpotUPDATE=1;
	%let minDataENDDATE=1;
	%let maxDataENDDATE=1;
	
	proc sql noprint;
		select put(max(UPDATEDATETIME),15.0),put(min(ENDDATE),15.0),put(max(ENDDATE),15.)
				into :maxPSSpotUPDATE,:minDataENDDATE,:maxDataENDDATE
		from AddSpotPriceDataTable;
	quit;
	
	%let reloadData=0;
	
	data _null_;
		if intnx('dtday',&scheduledRunDateTimeNum.,-1,'b') gt
				&maxPSSpotUPDATE. then reloadData=1;
		else reloadData=0;
		call symputx('reloadData',reloadData);
		call symputx('minDateToReload',max( 1, intnx('dtmonth',&maxDataENDDATE.,-1,'b')) );
	run;
	
	/*  SNB updated 2013-01-24 to prevent reload of data if attemptNumber gt 1 
		in case of nMarket database outages
	*/
	
	%if %sysevalf(&attemptNumber. gt 1) %then %let reloadData=0;
	
	%if %sysevalf(&reloadData. ne 0) %then %do;
		%let LookupIdValues = "&LookupIdValues";
		%GetInputTableData(InputTableMapId=&InputTableMapId.,
			                     InputTableIdDescription=SpotPriceDataTable,
								 LookupIdList=&LookupIdValues.,
								 outputTable=_OutputTable&currentSpotPriceId.,
							     dataStartDate=&minDateToReload.,
								 dataEndDate=&dataEndDate.);
	
		data _OutputTable&currentSpotPriceId.;
			set _OutputTable&currentSpotPriceId.(where=(price ne .)) AddSpotPriceDataTable;
			updateDateTime=updateDateTime;
			spotPriceId=&currentSpotPriceId.;
			if updateDateTime gt &scheduledRunDateTimeNum. then delete;
			if endDate gt intnx('dtday',&scheduledRunDateTimeNum.,0,'e') then delete;
		run;
	
		proc sort data=_OutputTable&currentSpotPriceId.;
			by spotPriceId /* startDate */ endDate updateDateTime;
		run;
	
		data _OutputTable&currentSpotPriceId.;
			set _OutputTable&currentSpotPriceId.;
			by spotPriceId /* startDate */ endDate;
	
			if startDate=endDate then delete;
	
			if last.endDate then output;
		run;
	
		proc sort data=_OutputTable&currentSpotPriceId. nodupkey;
			by spotPriceId startDate endDate;
		run;
	
		proc sort data=AddSpotPriceDataTable out=AddSpotPriceDataTableFinal;
			by spotPriceId /* startDate */ endDate updateDateTime;
		run;
	
		data AddSpotPriceDataTableFinal;
			set AddSpotPriceDataTableFinal;
			by spotPriceId /* startDate */ endDate;
	
			if startDate=endDate then delete;
	
			if last.endDate then output;
			
		run;
	
		proc sort data=AddSpotPriceDataTableFinal nodupkey;
			by spotPriceId startDate endDate;
		run;
	
		proc compare base=AddSpotPriceDataTableFinal compare=_OutputTable&currentSpotPriceId.
							out=NewPricesToAdd1 outcomp outnoequal
							noprint
							;
			by spotpriceid startdate enddate;
			var price;
		run;
	
		data NewPricesToAdd2;
			merge AddSpotPriceDataTableFinal(in=aa)
				  _OutputTable&currentSpotPriceId.(in=bb);
			by spotpriceid startdate enddate;
			if bb and not aa then output;
		run;
	
		data NewPricesToAdd;
			set NewPricesToAdd1 NewPricesToAdd2;
		run;
	
		proc sort data=NewPricesToAdd nodupkey;
			by spotPriceId startDate endDate;
		run;
	
		proc append data=NewPricesToAdd (drop=updateDateTime)
					base=powersim.spotpricedatatable force;
		run;
		quit;
	%end;
	
	%else %do;
	
		data _OutputTable&currentSpotPriceId.;
			set AddSpotPriceDataTable;
			updateDateTime=updateDateTime;
			spotPriceId=&currentSpotPriceId.;
			if updateDateTime gt &scheduledRunDateTimeNum. then delete;
			if endDate gt intnx('dtday',&scheduledRunDateTimeNum.,0,'e') then delete;
		run;
	
	%end;
	
%end;

%else %do;

	%GetInputTableData(InputTableMapId=&InputTableMapId.,
		                     InputTableIdDescription=SpotPriceDataTable,
							 LookupIdList=&LookupIdValues.,
							 outputTable=_OutputTable&currentSpotPriceId.,
						     dataStartDate=&dataStartDate.,
							 dataEndDate=&dataEndDate.);
	
	data _OutputTable&currentSpotPriceId.;
		set _OutputTable&currentSpotPriceId.(where=(price ne .)) AddSpotPriceDataTable;
		spotPriceId=&currentSpotPriceId.;
		if updateDateTime gt &scheduledRunDateTimeNum. then delete;
		if endDate gt intnx('dtday',&scheduledRunDateTimeNum.,0,'e') then delete;
	run;

%end;

%SysErrReturn;

proc sort data=_OutputTable&currentSpotPriceId.;
	by spotPriceId /* startDate */ endDate updateDateTime;
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
	eventDateTime=startDate;
	output;
	eventDateTime=EndDate;
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

	proc expand data=_OutputTable2x&currentSpotPriceId. out=_OutputTable2xEXP&currentSpotPriceId. to=hour1 method=step;
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

		if eventDateTime gt intnx('dtday',&scheduledRunDateTimeNum.,0,'e') then delete;
		format eventDateTime datetime.;
	run;


	proc append base=&OutputTable. data=_hourlyPrices&currentSpotPriceId.(obs=0) force;
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

%mend getSpotPriceData;
