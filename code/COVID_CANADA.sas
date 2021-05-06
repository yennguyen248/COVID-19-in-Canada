cas mysession;
caslib _all_ list;
caslib _all_ assign;

libname mycovid cas caslib=casuser;
%let mycovid=casuser;
%let tmplib=casuser;

*****************************/*Import Cases from Isha Berry Github*/********************************;
/*  */
/* FILENAME running url */
/* 'https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/cases.csv' debug; */
/* proc import datafile=running out=covid_case */
/* DBMS=csv REPLACE; */
/* GETNAMES=no; */
/* datarow=2; */
/* run; */
/*  */
/* data covid_case; */
/* set covid_case; */
/* seq=_n_; */
/* if seq =1 or seq=2 or seq =3 then delete; */
/* drop var13 var15 var16; */
/* rename var1=caseID var2=Prov_Case var3=age var4=sex var5=health_region var6=province var7=country var8=date_report */
/* var9=report_week var10=travel var11=travel_source var12=local_soure var14=additional; */
/* run; */


************************/*Import CASES_NEW from Isha Berry Github*/*********************************;

/**************************************/*Case2020*/************************************************/;
%let data='https://raw.githubusercontent.com/ccodwg/Covid19Canada/master/individual_level/cases_2020.csv';
filename t temp;

proc http method='get' url=&data. out=t TIMEOUT=120;
run;

/* create temppath in order to use server side cas upload */
%let temppath = %sysfunc(quote(%sysfunc(pathname(t))));

proc cas;
   session mysession;

/* Drop the previous version of this Global table in CAS Memory before loading it */
   table.dropTable / table="COVID_19_CANADA_CASE20_ABJ" quiet=TRUE;
run;

/* Import the csv into CAS via server side upload with GLOBAL scope as opposed to client side with PROC CASUTIL */
upload path=&temppath.                                         
   casOut={name='COVID_19_CANADA_CASE20_ABJ' replace=TRUE caslib="&mycovid"} 
	importOptions={fileType="csv"};
run;

/*Format Date*/
data &tmplib..covid_case20_abj;
	set mycovid.COVID_19_CANADA_CASE20_ABJ (datalimit=1000M);
	drop additional_info additional_source report_week method_note travel_yn;
	new_date=input(date_report, ddmmyy10.);
	format new_date date9.;
	drop date_report;
	rename new_date=date_report;
run;

/*  */
/* proc cas;             */
/*    datastep.runCode /                        */
/*    code = "data covid_case20_abj (replace=yes); */
/*            set mycovid.COVID_19_CANADA_CASE20_ABJ; */
/*            drop additional_info additional_source report_week method_note travel_yn; */
/*            new_date=input(date_report,ddmmyy10.); */
/*            format new_date date9.; */
/*            drop date_report; */
/*            rename new_date=date_report; */
/*            run;"; */
/* run; */
/* quit;  */

/**************************************/*Case2021*/************************************************/;
%let data='https://raw.githubusercontent.com/ccodwg/Covid19Canada/master/individual_level/cases_2021.csv';
filename t temp;

proc http method='get' url=&data. out=t TIMEOUT=120;
run;

/* create temppath in order to use server side cas upload */
%let temppath = %sysfunc(quote(%sysfunc(pathname(t))));

proc cas;
   session mysession;

/* Drop the previous version of this Global table in CAS Memory before loading it */
   table.dropTable / table="COVID_19_CANADA_CASE21_ABJ" quiet=TRUE;
run;

/* Import the csv into CAS via server side upload with GLOBAL scope as opposed to client side with PROC CASUTIL */
upload path=&temppath.                                         
   casOut={name='COVID_19_CANADA_CASE21_ABJ' replace=TRUE caslib="&mycovid"} 
	importOptions={fileType="csv"};
run;

/*Format Date*/
data &tmplib..covid_case21_abj;
	set mycovid.COVID_19_CANADA_CASE21_ABJ (datalimit=1000M);
	drop additional_info additional_source report_week method_note travel_yn;
	new_date=input(date_report, ddmmyy10.);
	format new_date date9.;
	drop date_report;
	rename new_date=date_report;
run;
/*  */
/* proc cas;             */
/*    datastep.runCode /                        */
/*    code = "data covid_case21_abj (replace=yes); */
/*            set &mycovid..COVID_19_CANADA_CASE21_ABJ; */
/*            drop additional_info additional_source report_week method_note travel_yn; */
/*            new_date=input(date_report,ddmmyy10.); */
/*            format new_date date9.; */
/*            drop date_report; */
/*            rename new_date=date_report; */
/*            run;"; */
/* run; */
/* quit;  */

/****************************/*COMBINE Case2020 and Case2021*/**********************************/;
data mycovid.covid_cases_abj (replace=yes);
	set &tmplib..COVID_CASE20_ABJ
		   &tmplib..COVID_CASE21_ABJ;
run;
/*  */
/* proc cas;             */
/*    datastep.runCode /                        */
/*    code = "data covid_cases_abj (replace=yes); */
/*            set &mycovid..COVID_CASE20_ABJ */
/* 		   &mycovid..COVID_CASE21_ABJ; */
/*            run;"; */
/* run; */
/* quit;  */

/**************************/*Finalize Case table and cleanup/**********************************/;
/*Promote the final case table*/
proc casutil;
%if %sysfunc(exist(&mycovid..COVID_19_CANADA_CASE_ABJ)) %then %do;
droptable casdata="COVID_19_CANADA_CASE_ABJ" incaslib="&mycovid" quiet;
promote casdata="COVID_CASES_ABJ" incaslib="&mycovid" outcaslib="&mycovid" casout="COVID_19_CANADA_CASE_ABJ";
%end;
%else %do;
promote casdata="COVID_CASES_ABJ" incaslib="&mycovid" outcaslib="&mycovid" casout="COVID_19_CANADA_CASE_ABJ";
%end;

/*Clean up tables that won't be used in the later part*/
/*Drop if the file already exists*/
%if %sysfunc(exist(&mycovid..COVID_19_CANADA_CASE20_ABJ)) %then %do;
proc delete data=&mycovid..COVID_19_CANADA_CASE20_ABJ;
run;
%end;
%if %sysfunc(exist(&mycovid..COVID_19_CANADA_CASE21_ABJ)) %then %do;
proc delete data=&mycovid..COVID_19_CANADA_CASE21_ABJ;
run;
%end;
%if %sysfunc(exist(&mycovid..COVID_CASE20_ABJ)) %then %do;
proc delete data=&mycovid..COVID_CASE20_ABJ;
run;
%end;
%if %sysfunc(exist(&mycovid..COVID_CASE21_ABJ)) %then %do;
proc delete data=&mycovid..COVID_CASE21_ABJ;
run;
%end;


************************/*Import Deaths from Isha Berry Github*/*********************************;
/*  */
/* FILENAME running url */
/* 'https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/mortality.csv' debug; */
/* proc import datafile=running out=covid_deaths */
/* DBMS=csv REPLACE; */
/* GETNAMES=no; */
/* datarow=2; */
/* run; */
/*  */
/* data covid_deaths; */
/* set covid_deaths; */
/* seq=_n_; */
/* if seq =1 or seq=2 or seq =3 then delete; */
/* drop var10 var12 var13; */
/* rename var1=deathID var2=Prov_Death var3=caseID var4=age var5=sex var6=health_region var7=province  */
/* var8=country var9=date_report var11=additional; */
/* run; */


************************/*Import Deaths 2020 from Isha Berry Github*/*****************************;

%let data='https://raw.githubusercontent.com/ccodwg/Covid19Canada/master/individual_level/mortality_2020.csv';
filename t temp;

proc http method='get' url=&data. out=t TIMEOUT=1;
run;

/* create temppath in order to use server side cas upload */
%let temppath = %sysfunc(quote(%sysfunc(pathname(t))));

proc cas;
   session mysession;

/* Drop the previous version of this Global table in CAS Memory before loading it */
   table.dropTable / table="COVID_19_CANADA_DEATH20_ABJ" quiet=TRUE;
run;

/* Import the csv into CAS via server side upload with GLOBAL scope as opposed to client side with PROC CASUTIL */
upload path=&temppath.                                         
   casOut={name='COVID_19_CANADA_DEATH20_ABJ' replace=TRUE caslib="&mycovid"} 
	importOptions={fileType="csv"};
run;

/*Format Date*/
data &tmplib..covid_deaths20_abj;
	set mycovid.COVID_19_CANADA_DEATH20_ABJ (datalimit=1000M);
	drop additional_info additional_source;
	new_date=input(date_death_report, ddmmyy10.);
	format new_date date9.;
	drop date_death_report;
	rename new_date=date_report;
run;

/*  */
/* proc cas;             */
/*    datastep.runCode /                        */
/*    code = "data covid_deaths20_abj (replace=yes); */
/*            set &mycovid..COVID_19_CANADA_DEATH20_ABJ; */
/*            drop additional_info additional_source; */
/*            new_date=input(date_death_report,ddmmyy10.); */
/*            format new_date date9.; */
/*            drop date_death_report; */
/*            rename new_date=date_report; */
/*            run;"; */
/* run; */
/* quit;  */


************************/*Import Deaths 2021 from Isha Berry Github*/*****************************;

%let data='https://raw.githubusercontent.com/ccodwg/Covid19Canada/master/individual_level/mortality_2021.csv';
filename t temp;

proc http method='get' url=&data. out=t TIMEOUT=1;
run;

/* create temppath in order to use server side cas upload */
%let temppath = %sysfunc(quote(%sysfunc(pathname(t))));

proc cas;
   session mysession;

/* Drop the previous version of this Global table in CAS Memory before loading it */
   table.dropTable / table="COVID_19_CANADA_DEATH21_ABJ" quiet=TRUE;
run;

/* Import the csv into CAS via server side upload with GLOBAL scope as opposed to client side with PROC CASUTIL */
upload path=&temppath.                                         
   casOut={name='COVID_19_CANADA_DEATH21_ABJ' replace=TRUE caslib="&mycovid"} 
	importOptions={fileType="csv"};
run;

/*Format Date*/
data &tmplib..covid_deaths21_abj;
	set mycovid.COVID_19_CANADA_DEATH21_ABJ;
	drop additional_info additional_source;
	new_date=input(date_death_report, ddmmyy10.);
	format new_date date9.;
	drop date_death_report;
	rename new_date=date_report;
run;

/* proc cas;             */
/*    datastep.runCode /                        */
/*    code = "data covid_deaths21_abj (replace=yes); */
/*            set &mycovid..COVID_19_CANADA_DEATH21_ABJ; */
/*            drop additional_info additional_source; */
/*            new_date=input(date_death_report,ddmmyy10.); */
/*            format new_date date9.; */
/*            drop date_death_report; */
/*            rename new_date=date_report; */
/*            run;"; */
/* run; */
/* quit;  */

/****************************/*COMBINE Deaths20 and Deaths21*/**********************************/;

data mycovid.covid_deaths_abj (replace=yes);
	set &tmplib..COVID_DEATHS20_ABJ
		   &tmplib..COVID_DEATHS21_ABJ;
run;

/* proc cas;             */
/*    datastep.runCode /                        */
/*    code = "data covid_deaths_abj (replace=yes); */
/*            set &mycovid..COVID_DEATHS20_ABJ */
/* 		   &mycovid..COVID_DEATHS21_ABJ; */
/*            run;"; */
/* run; */
/* quit;  */

/**************************/*Finalize Death table and cleanup/**********************************/;
/*Promote the final death table*/
proc casutil;
%if %sysfunc(exist(&mycovid..COVID_19_CANADA_DEATH_ABJ)) %then %do;
droptable casdata="COVID_19_CANADA_DEATH_ABJ" incaslib="&mycovid" quiet;
promote casdata="COVID_DEATHS_ABJ" incaslib="&mycovid" outcaslib="&mycovid" casout="COVID_19_CANADA_DEATH_ABJ";
%end;
%else %do;
promote casdata="COVID_DEATHS_ABJ" incaslib="&mycovid" outcaslib="&mycovid" casout="COVID_19_CANADA_DEATH_ABJ";
%end;

/*Clean up tables that won't be used in later part*/
%if %sysfunc(exist(&mycovid..COVID_19_CANADA_DEATH20_ABJ)) %then %do;
proc delete data=&mycovid..COVID_19_CANADA_DEATH20_ABJ;
run;
%end;
%if %sysfunc(exist(&mycovid..COVID_19_CANADA_DEATH21_ABJ)) %then %do;
proc delete data=&mycovid..COVID_19_CANADA_DEATH21_ABJ;
run;
%end;
%if %sysfunc(exist(&mycovid..COVID_DEATHS20_ABJ)) %then %do;
proc delete data=&mycovid..COVID_DEATHS20_ABJ;
run;
%end;
%if %sysfunc(exist(&mycovid..COVID_DEATHS21_ABJ)) %then %do;
proc delete data=&mycovid..COVID_DEATHS21_ABJ;
run;
%end;


****************/*Import Recovered from Isha Berry Github*/*************************************;
/*Drop if the file already exists*/
%if %sysfunc(exist(&mycovid..COVID_19_CANADA_RECOVERED)) %then %do;

proc delete data=&mycovid..COVID_19_CANADA_RECOVERED;
run;

%end;

%let data='https://raw.githubusercontent.com/ccodwg/Covid19Canada/master/timeseries_prov/recovered_timeseries_prov.csv';
filename t temp;

proc http method='get' url=&data. out=t TIMEOUT=1;
run;

/* create temppath in order to use server side cas upload */
%let temppath = %sysfunc(quote(%sysfunc(pathname(t))));

proc cas;
   session mysession;

/* Drop the previous version of this Global table in CAS Memory before loading it */
   table.dropTable / table="COVID_19_CANADA_RECOVERED" quiet=TRUE;
run;

/* Import the csv into CAS via server side upload with GLOBAL scope as opposed to client side with PROC CASUTIL */
upload path=&temppath.                                         
   casOut={name='COVID_19_CANADA_RECOVERED' replace=TRUE caslib="&mycovid"} 
	importOptions={fileType="csv"};
run;

data covid_recovered;
set mycovid.COVID_19_CANADA_RECOVERED;
run;


****************/*Import Testing from Isha Berry Github*/*************************************;

/*Drop if the file already exists*/
%if %sysfunc(exist(&mycovid..COVID_19_CANADA_TEST)) %then %do;

proc delete data=&mycovid..COVID_19_CANADA_TEST;
run;

%end;

%let data='https://raw.githubusercontent.com/ccodwg/Covid19Canada/master/timeseries_prov/testing_timeseries_prov.csv';
filename t temp;

proc http method='get' url=&data. out=t TIMEOUT=1;
run;

/* create temppath in order to use server side cas upload */
%let temppath = %sysfunc(quote(%sysfunc(pathname(t))));

proc cas;
   session mysession;
/* Drop the previous version of this Global table in CAS Memory before loading it */
   table.dropTable / table="COVID_19_CANADA_TEST" quiet=TRUE;
run;

/* Import the csv into CAS via server side upload with GLOBAL scope as opposed to client side with PROC CASUTIL */
upload path=&temppath.                                         
   casOut={name='COVID_19_CANADA_TEST' caslib="&mycovid" replace=true} 
	importOptions={fileType="csv"};
run;

data covid_test;
set mycovid.COVID_19_CANADA_TEST;
run;

*******************************/*Calculate Cases by Day & Deaths by Day*/***********************************************;

/*Create aggregate datasets from individual-level Cases data*/
proc sql;
create table cases as select date_report as Date, Health_region as Region, 
Province as Province,count(distinct case_ID) as Number_cases
from mycovid.COVID_19_CANADA_CASE_ABJ
group by Province, Region, Date;
quit;

proc sort data=cases out=cases nodupkey;
                by Region Province Date;
run;

/* data cases; */
/* set cases; */
/* new_date=input(date,ddmmyy10.); */
/* format new_date date9.; */
/* drop Date; */
/* rename new_date=date; */
/* run; */

/*Create aggregate datasets from individual-level Deaths data*/

proc sql;
create table deaths as select date_report as Date, Health_region as Region, 
Province as Province,count(distinct death_ID) as Number_deaths
from mycovid.COVID_19_CANADA_DEATH_ABJ
group by Province, Region, Date;
quit;

proc sort data=deaths out=deaths nodupkey;
                by Region Province Date;
run;

/* data deaths; */
/* set deaths; */
/* new_date=input(date,ddmmyy10.); */
/* format new_date date9.; */
/* drop Date; */
/* rename new_date=date; */
/* run; */

/*Subset Cases RBD data*/

proc sql;
                create table have as select max(Date) as max_date format=date9., 
                                min(Date) as min_date format=date9., Date as Date, 
                                Number_cases as Value, cats(Region, '/', Province) as Name from 
                                work.cases;
quit;

proc sort data=have out=uni(keep=Name min_date max_date) nodupkey;
                by Name;
run;

data dumpData;
                set uni;
                format Date date9.;
                do i=min_date to max_date;
                                Date=i;
                                output;
                                drop i min_date max_date;
                end;
run;

proc sort data=have out=have;
                by Name Date;
run;

data prep;
                merge have dumpData;
                by Name Date;

                if value=. then
                                value=0;
                drop min_date max_date;
run;

data prep;
                set prep;
                rename Name=Region_Province date=Date
                                value=Count_confirmed_cases;
run;

data prep;
                set prep;
                by Region_Province Date;
                retain cum_case;

                if first.Region_Province then
                                cum_case=Count_confirmed_cases;
                else
                                cum_case=Count_confirmed_cases + cum_case;
run;

proc sort data=prep out=try;
                by Region_Province Date;
run;

data prep_case;
                set work.try;
                by Region_Province Date;
                format l_case 8. growth comma12.2;
                l_case=lag(cum_case);

                if first.Region_Province then
                                growth=0;

                if not first.Region_Province then
                                do;
                                                growth=(cum_case-l_case)/l_case;
                                end;
run;

/*Subset Deaths RBD data*/

proc sql;
                create table have as select max(Date) as max_date format=date9., 
                                min(Date) as min_date format=date9., Date as Date, 
                                Number_deaths as Value, cats(Region, '/', Province) as Name from 
                                work.deaths;
quit;

proc sort data=have out=uni(keep=Name min_date max_date) nodupkey;
                by Name;
run;

data dumpData;
                set uni;
                format Date date9.;

                do i=min_date to max_date;
                                Date=i;
                                output;
                                drop i min_date max_date;
                end;
run;

proc sort data=have out=have;
                by Name Date;
run;

data prep;
                merge have dumpData;
                by Name Date;

                if value=. then
                                value=0;
                drop min_date max_date;
run;

data prep;
                set prep;
                rename Name=Region_Province date=Date
                                value=Count_confirmed_deaths;
run;

data prep;
                set prep;
                by Region_Province Date;
                retain cum_death;

                if first.Region_Province then
                                cum_death=Count_confirmed_deaths;
                else
                                cum_death=Count_confirmed_deaths + cum_death;
run;

proc sort data=prep out=try;
                by Region_Province Date;
run;

data prep_death;
                set work.try;
                by Region_Province Date;
                format l_death 8. growth comma12.2;
                l_death=lag(cum_death);

                if first.Region_Province then
                                growth=0;

                if not first.Region_Province then
                                do;
                                                growth=(cum_death-l_death)/l_death;
                                end;
run;

/*Aggregate to Region level for only Cases and Deaths*/

proc sql;
create table join as
select t1.Region_Province as Region_province,
t1.Date,t1.Count_confirmed_cases as cases,
t1.cum_case, t1.growth as growth_case,
t2.Region_province as Reg_Prov_1,
t2.Date as date1, t2.count_confirmed_deaths as deaths,
t2.cum_death,t2.growth as growth_deaths
from prep_case t1 left join prep_death t2 on (t1.Region_province=t2.region_province)
and (t1.date=t2.date);
quit;
run;

data join;
set join;
drop Reg_Prov_1 date1;
run;

/*Reform Recover and Test for Province_level aggregation*/
/*Recovered*/
data rec;
set covid_recovered;
new_date=input(date_recovered,ddmmyy10.);
format new_date date9.;
drop date_recovered;
rename new_date=date_rec;
cum_rec_new=input(cumulative_recovered, comma12.0);
format cum_rec_new comma12.0;
drop cumulative_recovered;
rename cum_rec_new=cum_rec;
run;

data rec;
set rec;
format Prov_ABB $20.;
if province="Ontario" then Prov_ABB="ON";
else if province="Quebec" then Prov_ABB="QC";
else if province="Alberta" then Prov_ABB="AB";
else if province="BC" then Prov_ABB="BC";
else if province="Manitoba" then Prov_ABB="MB";
else if province="New Brunswick" then Prov_ABB="NB";
else if province="Saskatchewan" then Prov_ABB="SK";
else if province="Yukon" then Prov_ABB="YT";
else if province="NWT" then Prov_ABB="NT";
else if province="NL" then Prov_ABB="NL";
else if province="PEI" then Prov_ABB="PE";
else if province="Nova Scotia" then Prov_ABB="NS";
else if province="Nunavut" then Prov_ABB="NU";
else Prov_ABB=province;
run;

proc sql;
create table prep_rec as
select distinct Prov_ABB as Prov,
max(cum_rec) as Recovered
from work.rec
group by Prov;
quit;

/*Tests*/
data test;
set covid_test;
new_date=input(date_testing,ddmmyy10.);
format new_date date9.;
drop date_testing;
rename new_date=date_test;
cum_test_new=input(cumulative_testing, comma12.0);
format cum_test_new comma12.0;
drop cumulative_testing;
rename cum_test_new=cum_test;
run;

data test;
set test;
format Prov_ABB $20.;
if province="Ontario" then Prov_ABB="ON";
else if province="Quebec" then Prov_ABB="QC";
else if province="Alberta" then Prov_ABB="AB";
else if province="BC" then Prov_ABB="BC";
else if province="Manitoba" then Prov_ABB="MB";
else if province="New Brunswick" then Prov_ABB="NB";
else if province="Saskatchewan" then Prov_ABB="SK";
else if province="Yukon" then Prov_ABB="YT";
else if province="NWT" then Prov_ABB="NT";
else if province="NL" then Prov_ABB="NL";
else if province="PEI" then Prov_ABB="PE";
else if province="Nova Scotia" then Prov_ABB="NS";
else if province="Nunavut" then Prov_ABB="NU";
else Prov_ABB=province;
run;

proc sql;
create table prep_test as
select distinct Prov_ABB as Prov,
max(cum_test) as Tests
from work.test
group by Prov;
quit;

/* Drop if the file already exists */
/* %if %sysfunc(exist(jsg_covid19_prod.COVID_19_CD_AB_test)) %then %do; */
/*  */
/* proc delete data=jsg_covid19_prod.COVID_19_CD_AB_test; */
/* run; */
/*  */
/* %end; */
/*  */
/* Load and Promote into CAS */
/* proc casutil; */
/*            load data=work.join casout="COVID_19_CD_AB_test" outcaslib=jsg_covid19_prod; */
/* run; */
/*  */
/* proc casutil; */
/*            promote incaslib="jsg_covid19_prod" casdata="COVID_19_CD_AB_test"  */
/*                            outcaslib="jsg_covid19_prod" casout="COVID_19_CD_AB_test"; */
/*            quit; */

/*SPLIT REGION_PROVINCE*/
data join;
set join;
drop temp len;
length Region $1024 Province $1024;
temp = kindexc("Region_province"n, "/");
len = length("Region_province"n);
if temp <= 0 then do;
Region = "Region_province"n;
Province = "";
end;
else do;
Region= ksubstr("Region_province"n, 1, temp-1);
Province = ksubstr("Region_province"n, temp+1, len-temp );
end;
run;

/*Drop if the file already exists*/
%if %sysfunc(exist(&mycovid..COVID_19_CD_AB_NEW)) %then %do;
proc delete data=&mycovid..COVID_19_CD_AB_NEW;
run;
%end;

/*Load and save into CAS file system*/
proc casutil;
                load data=work.join casout="COVID_19_CD_AB_NEW" outcaslib=&mycovid replace;
run;

/* proc casutil; */
/*            promote incaslib="jsg_covid19_prod" casdata="COVID_19_CD_AB_NEW_test"  */
/*                            outcaslib="jsg_covid19_prod" casout="COVID_19_CD_AB_NEW_test"; */
/*            quit; */
proc casutil;
    save casdata="COVID_19_CD_AB_NEW" incaslib="&mycovid" outcaslib="&mycovid"
                     casout="COVID_19_CD_AB_NEW" replace;
run;


/******************************************RECOVER AND TEST tables: Load & Promote **********************/

/*Drop if the file already exists*/
%if %sysfunc(exist(&mycovid..COVID_19_REC_AB)) %then %do;
proc delete data=&mycovid..COVID_19_REC_AB;
run;
%end;

/*Load and save into CAS file system*/
proc casutil;
                load data=work.prep_rec casout="COVID_19_REC_AB" outcaslib=&mycovid replace;
run;

/* proc casutil; */
/*            promote incaslib="jsg_covid19_prod" casdata="COVID_19_REC_AB"  */
/*                            outcaslib="jsg_covid19_prod" casout="COVID_19_REC_AB"; */
/*            quit; */

proc casutil;
    save casdata="COVID_19_REC_AB" incaslib="&mycovid" outcaslib="&mycovid"
                     casout="COVID_19_REC_AB" replace;
run;

/*Drop if the file already exists*/
%if %sysfunc(exist(&mycovid..COVID_19_TEST_AB)) %then %do;
proc delete data=&mycovid..COVID_19_TEST_AB;
run;
%end;

/*Load and save into CAS file system*/
proc casutil;
                load data=work.prep_test casout="COVID_19_TEST_AB" outcaslib=&mycovid replace;
run;

/* proc casutil; */
/*            promote incaslib="jsg_covid19_prod" casdata="COVID_19_TEST_AB"  */
/*                            outcaslib="jsg_covid19_prod" casout="COVID_19_TEST_AB"; */
/*            quit; */

proc casutil;
    save casdata="COVID_19_TEST_AB" incaslib="&mycovid" outcaslib="&mycovid"
                     casout="COVID_19_TEST_AB" replace;
run;

/*Delete unnecessary tables*/

/*Drop if the file already exists*/
%if %sysfunc(exist(&mycovid..COVID_19_CANADA_RECOVERED)) %then %do;

proc delete data=&mycovid..COVID_19_CANADA_RECOVERED;
run;

%end;
/*Drop if the file already exists*/
%if %sysfunc(exist(&mycovid..COVID_19_CANADA_TEST)) %then %do;

proc delete data=&mycovid..COVID_19_CANADA_TEST;
run;

%end;

