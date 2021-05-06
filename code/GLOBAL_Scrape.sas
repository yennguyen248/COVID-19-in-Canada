cas mysession sessopts=(caslib=casuser timeout=1800 locale="en_US");
caslib _all_ assign;

/*macro for Caslib*/
%let mycovid=CASUSER;


options mprint mlogic;

%macro COVID_19(outpath);
	%let url1=https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv;
	%let url2=https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv;
	%let url3=https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv;

%macro getcovid_19(url,dataset);
	filename data "%sysfunc(pathname(work))\covid-19.csv";

	proc http method="get" url="&url" out=data;
	run;

	data data0;
		infile data length=len lrecl=32767;
		input line $varying32767. len;
		line = strip(line);

		if len>0;
	run;

	data _null_;
		set data0;

		if line='<tr id="LC1" class="js-file-line">' then
			call symputx('n1',_n_);

		if line='</table>' then
			call symputx("n2",_n_);
	run;

	data data0;
		set data0;
		n=_n_;

		if &n1<=n<=&n2 then
			output;
		drop n;
	run;

	data data0;
		set data0;

		if prxmatch('/^<tr id="LC/',line) then
			newcol=1;
	run;

	data data0;
		set data0;
		retain group;

		if newcol=1 then
			do;
				group+1;
			end;

		drop newcol;
	run;

	data data1;
		set data0;

		if line in ("</thead>","<tbody>","</tbody>","</table>") then
			delete;
	run;

	data data2;
		set data1;
		by group;
		retain id;

		if first.group then
			id=0;
		id+1;
	run;

	data data3;
		set data2;

		if id=3 then
			_name_="Province_State";

		if id=4 then
			_name_="Country_Region";

		if id=5 then
			_name_="Lat";

		if id=6 then
			_name_="Long";

		if 3<=id<=6 then
			output;
	run;

	proc transpose data=data3 out=data4;
		where group gt 1;
		id _name_;
		var line;
		by group;
	run;

	data data4;
		set data4(rename=(lat=lat1 long=long1));

		do list = '<td>','</td>','>';
			province_state= tranwrd(strip(province_state),strip(list),'');
			country_region=tranwrd(strip(country_region),strip(list),'');
			lat1=tranwrd(strip(lat1),strip(list),'');
			long1=tranwrd(strip(long1),strip(list),'');
		end;

		lat=input(lat1,7.4);
		long=input(long1,8.4);
		drop _name_ lat1 long1;
	run;

	data data5;
		set data2;
		by group;

		if id<=6 or line="</tr>" then
			delete;
		where group >1;
	run;

	%let var=%scan(%scan(&url,-2,"./-"),-2,"_");
	%put &var;

	data &dataset.;
		merge data4 data5;
		by group;
		line=compress(line,"<td></td>");
		&var=input(line,8.0);
		retain date;

		if first.group then
			date='21jan2020'd;
		date+1;
		drop id line;
		format date yymmdd10.;
	run;

	proc datasets lib=WORK nolist;
		delete data0-data5;
	run;

%mend getcovid_19;

%getcovid_19(&url1,confirmed);
%getcovid_19(&url2,deaths);
%getcovid_19(&url3,recovered);

data COVID_19;
	set confirmed(keep=province_state country_region lat long confirmed);
	set deaths(keep=deaths);
	set recovered(keep=recovered date group);
run;

data covid_19;
	set covid_19;
	by group;

	if country_region not in ("Mainland China","China") then
		do;
			incc=confirmed-lag1(confirmed);
			incd=deaths-lag1(deaths);
			incr=recovered-lag1(recovered);
		end;

	if first.group and country_region not in ("Mainland China","China") then
		do;
			incc=confirmed;
			incd=deaths;
			incr=recovered;
		end;

	drop group;
run;

proc export data=COVID_19 outfile="&outpath" dbms=csv replace;
run;

%mend COVID_19;

%COVID_19(/enable01-export/enable01-aks/homes/Yen.Nguyen@sas.com/COVID_CANADA/covid_19.csv);

proc sql;
%if %sysfunc(exist(&mycovid..COVID_19_GLOBAL)) %then %do;
    drop table &mycovid..COVID_19_GLOBAL;
%end;
%if %sysfunc(exist(&mycovid..COVID_19_GLOBAL,VIEW)) %then %do;
    drop view &mycovid..COVID_19_GLOBAL;
%end;
quit;


FILENAME REFFILE DISK '/enable01-export/enable01-aks/homes/Yen.Nguyen@sas.com/COVID_CANADA/covid_19.csv';

PROC IMPORT DATAFILE=REFFILE
	DBMS=csv
	OUT=&mycovid..COVID_19_GLOBAL;
	GETNAMES=YES;
RUN;

PROC CONTENTS DATA=&mycovid..COVID_19_GLOBAL; RUN;

proc casutil;
	promote incaslib="&mycovid" casdata="COVID_19_GLOBAL" 
		outcaslib="&mycovid" casout="COVID_19_GLOBAL";
	quit;

cas mysession terminate;



