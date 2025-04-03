package org.example;
import org.apache.spark.sql.*;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("CompanyAggregation")
                .master("local[*]")
                .getOrCreate();

        //Locatia fisierului .parquet
        Dataset<Row> df = spark.read().parquet("locatia fisierului .parquet");

        //Listeaza coloanele pe care trebuie să le agregam
        String[] columns = {
                "company_name", "company_legal_names", "company_commercial_names",
                "main_country_code", "main_country", "main_region", "main_city_district",
                "main_city", "main_postcode", "main_street", "main_street_number",
                "main_latitude", "main_longitude", "main_address_raw_text", "locations"
        };

        //Folosim coloana company_name drept uniqueId
        Column companyId = col("company_name");

        Column[] aggExpressions = new Column[columns.length * 2]; //*2 pentru ca avem si coloanele "_other_values"
        for (int i = 0; i < columns.length; i++) {
            String colName = columns[i];

            //Selectam prima valoare din coloana pentru fiecare rand care este non-null
            Column firstValueCol = first(col(colName), true).alias(colName + "_agg");

            //Construim expresia pentru "other_values": eliminam valoarea "firstValueCol" din lista de valori diferite din aceeasi coloana si acelasi "companyId"
            Column otherValuesCol = expr(
                    "array_except(collect_set(" + colName + "), array(first(" + colName + ", true)))"
            ).alias(colName + "_other_values");

            aggExpressions[i * 2] = firstValueCol;
            aggExpressions[i * 2 + 1] = otherValuesCol;
        }

        //Aplicam agregarea in toate coloanele
        /*
        * in loc de a mentiona fiecare coloana in parte, folosim "Arrays.copyOfRange(aggExpressions, 1, aggExpressions.length)"
        * pentru a copia coloanele de la al doilea element pana la sfarsit + daca o coloana noua este adaugata iar coloanele ar fi fost adaugate manual,
        * codul ar fi trebuit updatat
        * (primul element ("aggExpressions[0]") este salvat separat pentru ca ".agg" necesita cel putin un element)
        *
        * */
        Dataset<Row> aggregatedDF = df.groupBy(companyId).agg(aggExpressions[0], Arrays.copyOfRange(aggExpressions, 1, aggExpressions.length));

        //Salvam rezultatul intr-un fisier .parquet ("repartition(1)" este folosit pentru a salva rezultatele intr-un singur fisier)
        aggregatedDF.repartition(1).write().parquet("locatia unde fisierului .parquet sa fie salvat");

        spark.stop();
    }
}