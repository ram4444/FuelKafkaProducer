package main.kotlin

import org.springframework.boot.SpringApplication
import org.springframework.boot.WebApplicationType
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration
import org.springframework.scheduling.annotation.EnableScheduling


@SpringBootApplication
@EnableScheduling
class FuelKafkaProducer
fun main(args: Array<String>) {
    val app = SpringApplication(FuelKafkaProducer::class.java)
    app.webApplicationType = WebApplicationType.REACTIVE
    app.run(*args)
}
