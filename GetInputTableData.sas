%macro GetInputTableData(InputTableMapId=,
	                     InputTableIdDescription=,
						 LookupIdList=2 3,
						 outputTable=OutputTable,
					     dataStartDate=,
						 dataEndDate=,
						 SpotPriceId=);

%if &InputTableMapId= %then %do;

	proc sql noprint;
		select s.InputTableMapId into :InputTableMapId
		from powersim.InputTableMapIdTable s,
			 powersim.InputTableIdTable t
		where s.InputTableId=t.InputTableId and
			  s.DefaultFlag=1 and
			  upcase(compress(t.Description))=upcase(compress("&InputTableIdDescription"));
	quit;

%end;

proc sql noprint;

	create table InputTableMapIdTable as
	select *
	from powersim.InputTableMapIdTable
	where InputTableMapId=&InputTableMapId.;

	create table InputTableMapDataTable as
	select *
	from powersim.InputTableMapDataTable
	where InputTableMapId=&InputTableMapId.;

	select trim(left(inputTableName))||' as '||trim(left(PowerSimmName)) into :selectStatement separated by ','
	from InputTableMapDataTable;

quit;

proc transpose data=InputTableMapIdTable out=InputTableMapIdTableTransN;
var _numeric_;
run;

proc transpose data=InputTableMapIdTable out=InputTableMapIdTableTransC;
var _char_;
run;

data _null_;
	set InputTableMapIdTableTransN;
	call symput(trim(left(_name_)),trim(left(col1)) );
run;

data _null_;
	set InputTableMapIdTableTransC;
	call symput(trim(left(_name_)),trim(left(col1)) );
run;

%put TRAVERSE_API_TABLE_NAME = "Traverse API";
%if &TABLEMANE. ne &TRAVERSE_API_TABLE_NAME.  %then %do;
	proc sql;
		create table &outputTable. as
		select &selectStatement
		from &SASLIBNAME..&TABLENAME.
		where &LOOKUPIDVARIABLE. in (&LookupIdList.)
			%if %sysfunc(compress("&EXTENDEDQUERYSTRING.")) ne "" %then %do;
			   AND &EXTENDEDQUERYSTRING.
			%end;
	        %if %sysfunc(compress("&DATEVARIABLE.")) ne "" %then %do;
			  %if %sysfunc(compress("&dataStartDate.")) ne "" %then %do;
			       and &DATEVARIABLE. ge &dataStartDate.
			  %end;
			  %if %sysfunc(compress("&dataEndDate.")) ne "" %then %do;
			       and &DATEVARIABLE. le &dataEndDate.
			  %end;
			%end;
		;
	quit;
%end;
%else %do;
	%TraverseRequest(StartDate=&MostCurrentDate., EndDate=&dataEndDate.,SpotpriceId=&SpotpriceId.,Query=&lookupIdList,OutputTable=&outputTable.);
%mend GetInputTableData;
