Initial am citit problema si am inteles ca sunt companii cu informatii despre ele din mai multe surse, ceea ce face ca acea companie sa apara de mai multe ori in fisier si totodata cu informatii diferite in functie de sursa. Problema cere ca informatiile sa fie filtrare in asa fel incat fiecare companie sa apara doar o data.

Am deschis fisierul pentru a ma uita in el in asa fel incat sa inteleg mai bine problema si sa pot veni cu o solutie. Am luat numele companiei drept element unic si initial m-am gandit sa vin cu o solutie care sa combine coloana cu valoare daca aceasta este inainte era null pentru ca am crezut ca fiecare coloana pentru o companie poate sa fie null sau sa aiba aceasi valoare de fiecare data cand apare, dar de fapt o coloana poate sa aiba mai multe valori. Asa ca m-am gandit sa vin cu urmatoare solutie: In cazul in care o coloana are si alte valori, acestea sa apara in alta coloana in asa fel incat toate valorile din acea coloana sa fie salvate.

Nu am mai facut un exercitiu de acest fel asa ca am cautat pe internet ce tool-uri as putea folosi. Asa am ajuns la concluzia ca Apache Spark ar fi o unealta de care m-as putea folosi pentru ca permite agregarea datelor in mod eficient. Si pentru ca nu l-am mai folosit in trecut, a trebuit sa trec prin mai multe clipuri de pe Youtube sau pagini web pentru a reusi sa gasesc raspuns unde a fost nevoie sau pentru a rezolva diverse erori. Am incarcat datele dintr-un fisier .parquet intr-un Dataset<Row> pentru procesare. Am creat un array String pentru a stoca numele coloanelor dupa care am inceput sa iterez fiecare coloana in parte. In cazul in care o coloana cu acelasi companyId (care este numele companiei) a mai aparut si are alta valoare, valoarea recenta este trecuta in coloana "[nume_coloana]_other_values". Prin aceasta solutie am reusit sa reduc numarul de randuri de la aproximativ 33000 la 18000.

In final am creat un alt Dataset<Row> unde am sortat companiile dupa nume si am copiat valorile modificate.

Un mic rezumat unde as putea explica mai direct este ca am folosit "Column firstValueCol = first(col(colName), true).alias(colName + "_agg");" = pentru a selecta prima valoare non-null din coloana ca apoi sa folosesc "array_except(collect_set(col), array(first(col, true)))" pentru a colecta datele diferite in "[nume_coloana]_other_values", exceptie fiind prima valoare.

In cazul in care nu am explicat clar sau ceva nu s-a inteles, sunt deschis pentru intrebari.
