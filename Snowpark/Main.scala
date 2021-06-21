import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types._

object Main { 
  def main(args: Array[String]): Unit = {

    val builder = Session.builder.configs(Map(
        "URL" -> "",
        "USER" -> "",
        "PASSWORD" -> "",
        "ROLE" -> "",
        "WAREHOUSE" -> "",
        "DB" -> "",
        "SCHEMA" -> ""
    ))
      
    //CREATE SESSION
    val session = builder.create

    //CALCULATE STEPS BY DAY
    val steps = session.table("STEPCOUNT")
    val steps_daily_agg = steps.select(col("ID"), date_trunc("day", col("STARTTIME")) as "day", col("VALUE") as "steps")
                .filter(col("ID") === "K0FJO")
                .sort(col("day").desc)
                .groupBy(col("day")).agg(col("steps")->"sum")
                .select(col("day") as "DAY", col("SUM(STEPS)") as "TOTAL_STEPS");

    steps_daily_agg.write.mode("overwrite").saveAsTable("SNOWPARK.MARIUS.STEPS_AGG");
  
    ////

    //CALCULATE CONSUMED CALS BY DAY 
    val cal_table = session.table("DIETARYENERGY")

    val diet_cals_agg = cal_table.select(col("ID"), date_trunc("day", col("STARTTIME")) as "day", col("VALUE") as "cals")
                .filter(col("ID") === "K0FJO")
                .sort(col("day").desc)
                .groupBy(col("day")).agg(col("cals")->"sum")
                .select(col("day") as "DAY", col("SUM(CALS)") as "TOTAL_CONSUMED");

    diet_cals_agg.write.mode("overwrite").saveAsTable("SNOWPARK.MARIUS.DIET_CALS_AGG");

    ////

    //CALCULATE ACTIVE CALS BY DAY 
    val active_cals_table = session.table("ACTIVE_ENERGY_BURNED")

    val active_cals_agg = active_cals_table.select(col("ID"), date_trunc("day", col("STARTTIME")) as "day", col("VALUE") as "cals")
                .filter(col("ID") === "K0FJO")
                .sort(col("day").desc)
                .groupBy(col("day")).agg(col("cals")->"sum")
                .select(col("day") as "DAY", col("SUM(CALS)") as "TOTAL_ACTIVE");

    active_cals_agg.write.mode("overwrite").saveAsTable("SNOWPARK.MARIUS.ACTIVE_CALS_AGG");


    ////

    //CALCULATE BASAL CALS BY DAY 
    val basal_cals_table = session.table("BASALENERGYBURNED")

    val basal_cals_agg = basal_cals_table.select(col("ID"), date_trunc("day", col("STARTTIME")) as "day", col("VALUE") as "cals")
                .filter(col("ID") === "K0FJO")
                .sort(col("day").desc)
                .groupBy(col("day")).agg(col("cals")->"sum")
                .select(col("day") as "DAY", col("SUM(CALS)") as "TOTAL_BASAL");

    basal_cals_agg.write.mode("overwrite").saveAsTable("SNOWPARK.MARIUS.BASAL_CALS_AGG");

    /////

    val joined_together = steps_daily_agg
            .join(basal_cals_agg, steps_daily_agg.col("day") === basal_cals_agg.col("day"))
            .join(diet_cals_agg, steps_daily_agg.col("day") === diet_cals_agg.col("day"))
            .join(active_cals_agg, steps_daily_agg.col("day") === active_cals_agg.col("day"))

            .select(steps_daily_agg.col("day").as("day"), 
                    basal_cals_agg.col("TOTAL_BASAL").as("BASAL"),
                    active_cals_agg.col("TOTAL_ACTIVE").as("ACTIVE"),
                    diet_cals_agg.col("TOTAL_CONSUMED").as("CONSUMED") );
    
    joined_together.write.mode("overwrite").saveAsTable("SNOWPARK.MARIUS.ALL_DATA");






  }//end main

}