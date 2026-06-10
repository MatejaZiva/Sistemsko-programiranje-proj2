using Sistemsko_programiranje_projekat1;
//treba Loger da stavis kad se desi cache hit vreme kao da se pokaze kolko je brze cache nego api call
//proveri dal sam lepo tokene uradio
//cache stampede mislim da ne treba se dira posto niti await niti Taskovi pomazu tu\

/*
proveri generalno kod i sve logiku ja bih rekao da je okej i da sklonio sam onaj tvoj lep html jer isk mora dosta se smaram sa tim,
posto sad saljemo podatke po chunks ne pravimo json pa saljemo nego odma saljemo objekat
*/
//Video sam od nekih isto da imaju kao neku request QUeue, moze ako oces bacis oko na cs pa udjes rezultati projekta 2 deo i udjes na nekih sto su predali
//Dodaj ContinueWith negde treba se to doda
//I generalno proveri sve jer nisam nesto puno proveo vremena a bilo bi lepo da dobije 100 poena POSTO SMO 3 GODINA RI
//Thank you pooky bear <33333
//- Yours truly Zivadinovic Mateja ;3

AppSettings set = new AppSettings();
WebServer srv = new WebServer(set, "https://api.europeana.eu/record/v2/search.json");

srv.asyncStartTheServer().Wait();

