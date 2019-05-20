package main.kotlin.scheduledjob

import com.github.kittinunf.fuel.Fuel
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.scheduling.annotation.Scheduled
import java.time.LocalDateTime
import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.fuel.jackson.responseObject
import com.github.kittinunf.fuel.json.responseJson
import main.kotlin.config.KakfaConfig

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.bson.types.ObjectId
import org.json.JSONObject
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.io.File
import java.lang.Exception
import java.math.BigDecimal
import java.time.LocalDate
import java.time.ZoneId
import java.util.*


@Configuration
@ConfigurationProperties(prefix = "wtd")
@Service
class WorldTradingData {

    @Value("\${WorldTradingData.api_token}")                         val api_token: String = ""
    @Value("\${WorldTradingData.url_stock_history}")                 val url_stock_history: String = ""

    private val logger = KotlinLogging.logger {}

    @Autowired
    lateinit var kafkaTemplate: KafkaTemplate<String, GenericRecord>

    val zoneId = ZoneId.systemDefault()
    val zoneOffset = zoneId.getRules().getOffset(LocalDateTime.now())

    val schema_open = Schema.Parser().parse(File("src/main/resources/avsc/node-src-open.avsc"))

    /** Example
     * This @Schedule annotation run every 5 seconds in this case. It can also
     * take a cron like syntax.
     * See https://docs.spring.io/spring/docs/current/javadoc-api/org/springframework/scheduling/support/CronSequenceGenerator.html
     * "0 0 * * * *" = the top of every hour of every day.
        * "10 * * * * *" = every ten seconds.
        * "0 0 8-10 * * *" = 8, 9 and 10 o'clock of every day.
        * "0 0 6,19 * * *" = 6:00 AM and 7:00 PM every day.
        * "0 0/30 8-10 * * *" = 8:00, 8:30, 9:00, 9:30, 10:00 and 10:30 every day.
        * "0 0 9-17 * * MON-FRI" = on the hour nine-to-five weekdays
        * "0 0 0 25 12 ?" = every Christmas Day at midnight
     */


    @Scheduled(fixedRate = 5000000)
    fun getHistoryStock(){
        var symbol = "2888.HK"
        //if (!stockcode.isNullOrBlank()) symbol = stockcode

        var pindate = LocalDate.parse("1980-01-01")
        var i:Long=1

        /*
        https://www.worldtradingdata.com/api/v1/history?symbol=AAPL&sort=newest&api_token=demo
        {
            "name": "AAPL",
            "history": {
                2019-02-28: {
                    "open": "174.32",
                    "close": "173.15",
                    "high": "174.91",
                    "low": "172.92",
                    "volume": "28215416"
                },
                2019-02-26: {
                    "open": "173.71",
                    "close": "174.33",
                    "high": "175.30",
                    "low": "173.17",
                    "volume": "17070211"
                ...
            }
        }
         */
        val (request, response, result) = Fuel.get(
                url_stock_history,
                listOf("symbol" to symbol,
                        "sort" to "oldest",
                        "api_token" to api_token))
                .responseJson()
        //logger.debug{"Request: ${request}" }
        //logger.debug{"Response: ${response}" }
        //logger.debug{"Result: ${result.get().obj()}" }
        val obj = result.get().obj()

        // TODO: Undergoing ETL process to Schema Format, then to KAFKA

        val history = obj.getJSONObject("history")
        var open = BigDecimal(0)

        val stockname = obj.getString("name")
        //logger.debug { "history.length: ${history.length()}" }

        var nodeloopPinDate:Date?
        var nodeloopPinTSOpen:Date?

        while (pindate.isBefore(LocalDate.now()) ) {
            try {
                var dayObj :JSONObject = JSONObject()
                try {
                    dayObj = history.getJSONObject(pindate.toString())
                } catch (e:Exception) {
                    logger.info { "-------------------${pindate} have no record-------------------" }
                    //throw e.fillInStackTrace()
                }
                open = dayObj.getString("open").toBigDecimal().setScale(2, BigDecimal.ROUND_HALF_UP)
                //val timeSerialTypeOpen = TimeSerialType(ObjectId.get() ,"src_open_${stockname}","source",null)

                nodeloopPinDate = Date.from(pindate.atStartOfDay(zoneId).toInstant())
                nodeloopPinTSOpen = Date.from(pindate.atTime(9,0).toInstant(zoneOffset))

                // Define Topic Name in KakfaConfig

                //Please follow the local AVSC json
                val nodeOpen = GenericRecordBuilder(schema_open).apply {
                    set("id", ObjectId.get().toHexString())
                    set("value", open.toFloat())
                }.build()

                //TODO: put to Kafka producer, serialize the object
                kafkaTemplate.send(KakfaConfig.PRODUCER_SrcNodeOpen, nodeOpen)

            } catch (e:Exception) {
                logger.debug { "Error when executing ${pindate} of Stock $stockname: " }
                logger.error { "${e.message}" }
                //e.printStackTrace()
            } finally {
                pindate=pindate.plusDays(1)

            }

        }
    }
}