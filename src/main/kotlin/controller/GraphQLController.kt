package main.kotlin.controller


import graphql.schema.DataFetcher
import graphql.schema.StaticDataFetcher
import main.kotlin.graphql.GraphQLHandler
import main.kotlin.graphql.GraphQLRequest
import mu.KotlinLogging
import org.springframework.web.bind.annotation.*

import javax.annotation.PostConstruct

@RestController
class GraphQLController() {

    //@Autowired
    //val mongoDBservice: MongoDBService = MongoDBService()

    private val logger = KotlinLogging.logger {}

    //Initiate schema from somewhere
    val schema ="""
            type Query{
                query_func1: Int

            }"""

    lateinit var fetchers: Map<String, List<Pair<String, DataFetcher<out Any>>>>
    lateinit var handler:GraphQLHandler

    @PostConstruct
    fun init() {

        //initialize Fetchers
        fetchers = mapOf(
                "Query" to
                        listOf(
                                "query_func1" to StaticDataFetcher(999)
                        )
        )

        handler = GraphQLHandler(schema, fetchers)
    }

    @RequestMapping("/")
    suspend fun pingcheck():String {
        println("ping")
        logger.debug { "Debugging" }
        return "success"
    }
    @CrossOrigin(origins = arrayOf("http://localhost:3000"))
    @PostMapping("/graphql")
    fun executeGraphQL(@RequestBody request:GraphQLRequest):Map<String, Any> {

        val result = handler.execute(request.query, request.params, request.operationName, ctx = null)

        return mapOf("data" to result.getData<Any>())
    }

}
