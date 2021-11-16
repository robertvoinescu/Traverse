%macro GetTraverseRequest( StartDate=,
			EndDate=,
			SpotpriceId=,
			Query=,
			OutputTable=);
	/* 
	Returns all energy products for the specified node and iso found on Traverse.

	Params
	------
	StartDate: (str) A timestamp specifying the the start of the daterange lookup for data.
	EndDate: (str)  A timestamp specifying the the start of the daterange lookup for data.
	ISO: (str) The ISO (Independent Systems Operator) we wish to pull our data from
	Node: (str) The node for which we want to call the  
	OutputTable: (str) The name which we give the loaded sas data set

	Global Params
	-------------
	WorkingDirectory: Specifies the location of the working directory 
	BookMacroCodebase: Specifies the location of the codebase 
	OutputLogPath: Specifies where the logs are stored

	Necessary Tables
	----------------
	SpotPriceIdTable: specifies the correspondence between (iso,node,product) and spotpriceid

	Creates
	-------
	OutputTable: (sas dataset) the data loaded from Traverse

	Notes
	-----
	Currently this only loads in energy data.
	*/

	/* enesure log file does not overwrite a previous one */ 
	/*
	%let LogFile 	   = "&OutputLogPath\JobId"||trim(left("&JobId."))||"_"||compress(put(datetime(),datetime18.),' :')||"_traverse.log"; 
	%let TraverseRequestSubDir  = Powersimm\sasmacro\ForwardPriceSim;
	%let WorkDirectory = %sysfunc(getoption(work));
	*/
	
	%let LogFile 	   = "~\Desktop\log"; 
	%let TraverseRequestSubDir  = "~\Desktop";
	%let WorkDirectory = "~\Desktop";

	filename pos "&WorkDirectory.\GetTraverseRequest.bat";


	/*
	A bat file is created and run because trying to run the command directly with the x macro doesnt work in batch mode
	(although it will work interactively)
	*/
	data _null_;
		file pos;
		pythonpath = %sysfunc(quote("C:\Program Files\Python37\python.exe"));
		msgline = pythonpath || " &BookMacroCodeBase.\&TraverseRequestSubDir.\traverse_request.py --output-file &WorkDirectory.\&OutputFile..csv --log &WorkDirectory.\&LogFile. --start-date &StartDate. --end-date &EndDate. --spot-price-id &SpotpriceId  --powersimm-query '&Query.'";
		put msgline; 
	run;

	options noxwait xsync;
	x "&WorkDirectory.\GetTraverseRequest.bat";

	
	proc import datafile="&WorkDirectory.\&OutputFile..csv" out=&OutputFile. dbms=csv replace;
		guessingrows=max;
	run;

%mend GetTraverseRequestRunIt;
