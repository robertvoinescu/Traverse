%macro GetTraverseRequest( StartDate=,
			EndDate=,
			Query=,
			OutFile=);
	/* 
	Returns all energy products for the specified node and iso found on Traverse.

	Params
	------
	StartDate: (str) A timestamp specifying the the start of the daterange lookup for data
		in 01Jan2020:00:00:00 format.
	EndDate: (str)  A timestamp specifying the the start of the daterange lookup for data
		in 01Jan2020:00:00:00 format.
	Query: (str) specifies the iso node and output with format "iso=yourIso,node=yourNode,product=yourProduct"
	OutFile: (str) specifies the name of the output sas dataset 

	Creates
	-------
	OutputTable: (sas dataset) the data loaded from Traverse

	Notes
	-----
	Currently this only loads in energy_rt5, energy_rt15 and energy_da data.
	*/

	/* enesure log file does not overwrite a previous one */ 
	%let LogFile 	   = "&OutputLogPath\JobId"||trim(left("&JobId."))||"_"||compress(put(datetime(),datetime18.),' :')||"_traverse.log"; 
	%let WorkDirectory = %sysfunc(getoption(work));
	%let PythonTraverseRequestSubDir  = Powersimm\sasmacro\PowerSimmSystemTools\Python;
	
	/* query comes in quoted but bat files inherently include quotes around the parameters so need to remove here */
	%let Query = %sysfunc(dequote(&Query));


	/* for debugging purposes it is best to keep all the bat files using a counter suffix */
	%if %symexist(TraverseCallCounter)=0 %then %do;
		%global TraverseCallCounter;
		%let TraverseCallCounter=0;
	%end;
	%let TraverseCallCounter=%sysevalf(&TraverseCallCounter.+1);

	filename pos "&WorkDirectory.\GetTraverseRequest_&TraverseCallCounter..bat";

	/* A bat file is created and run because trying to run the command directly with the x macro doesnt work in batch mode
	   (although it will work interactively) */
	data _null_;
		file pos;
		pythonpath = %sysfunc(quote("C:\Program Files\Python37\python.exe"));
		msgline = pythonpath || " &BookMacroCodeBase.\&PythonTraverseRequestSubDir.\traverse_request.py --output-file &WorkDirectory.\&OutFile..csv --log &WorkDirectory.\&LogFile. --start-date &StartDate. --end-date &EndDate. --powersimm-query &Query.";
		put msgline; 
	run;

	options noxwait xsync;
	x "&WorkDirectory.\GetTraverseRequest_&TraverseCallCounter..bat";

	proc import datafile="&WorkDirectory.\&OutFile..csv" out=&OutFile. dbms=csv replace;
		guessingrows=max;
	run;

%mend GetTraverseRequest;

