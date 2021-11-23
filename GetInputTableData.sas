%macro GetInputTableData(InputTableMapId=,
	                     InputTableIdDescription=,
						 LookupIdList=2 3,
						 outputTable=OutputTable,
					     dataStartDate=,
						 dataEndDate=);

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


%if &TABLENAME. = TraverseAPI %then %do;
	%if %sysfunc(compress("&dataEndDate.")) = "" %then %do;
		%let dataEndDate= %sysfunc(datetime());
	%end;
	%let OneHourInSeconds = %sysevalf(60*50*24);
	%if %sysfunc(compress("&dataStartDate.")) = "" %then %do;
		%let dataStartDate= %eval(&dataEndDate.-&OneHourInSeconds.);
	%end;
	/* python doesn't understand sas input for datetime so need to convert
	   to datetime 20 formated characters */
	data _null_;
		dtCharStart=put(&dataStartDate., DATETIME20.);
		call symput('dataStartDate',dtCharStart);
		dtCharEnd=put(&dataEndDate., DATETIME20.);
		call symput('dataEndDate',dtCharEnd);
	run;
	%GetTraverseRequest(StartDate=&dataStartDate., EndDate=&dataEndDate.,Query=&LookupIdList,OutFile=&outputTable.);
%end;
%else %do;
	data _null_;
		MacroVariableNoQuotes = compress(&LookupIdList.,'"');
		call symput('LookupIdList',MacroVariableNoQuotes);
	run;
	
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
%mend GetInputTableData;

