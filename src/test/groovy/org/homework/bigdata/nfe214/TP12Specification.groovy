package org.homework.bigdata.nfe214

import org.apache.http.HttpHost
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.index.query.Operator
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder
import spock.lang.Shared
import spock.lang.Specification

// docker run -d --name remy-elastic -p 9200:9200 -p 9300:9300 elasticsearch
class TP12Specification extends Specification {

    @Shared
    RestHighLevelClient client

    def setupSpec() {
        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200, "http"),
                new HttpHost("localhost", 9300, "http")).build()

        client = new RestHighLevelClient(restClient)
    }

    // curl -XGET http://localhost:9200/movies/_search?q=fields.title:Star+Wars | jq
    // curl -XGET http://localhost:9200/movies/_search?q=title:alien,coppola | jq
    def "Première notion de score - Explain sur les films dont le titre contient life"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)
        searchSourceBuilder.explain(true)
        MatchQueryBuilder mqb = QueryBuilders.matchQuery("fields.title", "life")
        mqb.operator(Operator.AND)
        searchSourceBuilder.query(mqb)

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.getSourceAsMap().fields.title} ,${hit.getSourceAsMap().fields.year} \nExplanation\n========\n${hit.getExplanation()}"
        }
    }

    def "Utilisation du boosting - Recherche des films Star Wars et George Lucas"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)

        MatchQueryBuilder mqb1 = QueryBuilders.matchQuery("fields.title", "Star Wars")
        mqb1.boost(4)
        mqb1.operator(Operator.AND)

        MatchQueryBuilder mqb2 = QueryBuilders.matchQuery("fields.directors", "George Lucas")
        mqb2.operator(Operator.AND)

        searchSourceBuilder.query(QueryBuilders.boolQuery().should(mqb1).should(mqb2))

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.getSourceAsMap().fields.title} ,${hit.getSourceAsMap().fields.directors}"
        }
    }

    def "Utilisation du boosting negatif pour mettre en fin Abrams - Recherche des films Star Wars"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)

        MatchQueryBuilder mqb1 = QueryBuilders.matchQuery("fields.title", "Star Wars")
        mqb1.operator(Operator.AND)
        mqb1.boost(2.0)

        MatchQueryBuilder mqb2 = QueryBuilders.matchQuery("fields.directors", "J.J. Abrams")
        mqb2.operator(Operator.AND)

        //QueryBuilders.boostingQuery()
        searchSourceBuilder.query(QueryBuilders.boostingQuery(mqb1, mqb2).negativeBoost(0.5))

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.getSourceAsMap().fields.title} ,${hit.getSourceAsMap().fields.directors}"
        }
    }

    def "Recherche des films Star Wars pondérés en fonction de leur note"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)

        MatchQueryBuilder mqb1 = QueryBuilders.matchQuery("fields.title", "Star Wars")
        mqb1.operator(Operator.AND)

        FunctionScoreQueryBuilder fBuilder = QueryBuilders.functionScoreQuery(mqb1, ScoreFunctionBuilders.fieldValueFactorFunction("fields.rating"))

        searchSourceBuilder.query(fBuilder)

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.getSourceAsMap().fields.title} ,${hit.getSourceAsMap().fields.rating}"
        }
    }

    def "Recherche des films sortie oey de temps avant ou après 'Grand prix' de Frankenheimer, sorti le 21 décembre 1966"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)

        FunctionScoreQueryBuilder fBuilder = QueryBuilders.functionScoreQuery(QueryBuilders.existsQuery("fields.release_date"),
                ScoreFunctionBuilders.gaussDecayFunction("fields.release_date","1966-12-21T00:00:00Z","30d","1d",0.5))

        searchSourceBuilder.query(fBuilder)

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.getSourceAsMap().fields.title} ,${hit.getSourceAsMap().fields.release_date}"
        }
    }

    def "les films de James Cameron en pondérant négativement ceux qui durent plus de deux heures"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)

        MatchQueryBuilder mqb1 = QueryBuilders.matchQuery("fields.directors", "James Cameron")
        mqb1.operator(Operator.AND)

        FunctionScoreQueryBuilder fBuilder = QueryBuilders.functionScoreQuery(mqb1,
                ScoreFunctionBuilders.exponentialDecayFunction("fields.running_time_secs","7200","200", 0.5))

        searchSourceBuilder.query(fBuilder)

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.getSourceAsMap().fields.title} ,${hit.getSourceAsMap().fields.running_time_secs}"
        }
    }

    def "les meilleures comédie romantique"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)

        MatchQueryBuilder mqb1 = QueryBuilders.matchQuery("fields.genres", "Romance")
        searchSourceBuilder.sort("fields.rating", SortOrder.DESC)

        FunctionScoreQueryBuilder fBuilder = QueryBuilders.functionScoreQuery(mqb1,
                ScoreFunctionBuilders.exponentialDecayFunction("fields.running_time_secs","7200","200", 0.5))

        searchSourceBuilder.query(fBuilder)

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.getSourceAsMap().fields.title} ,${hit.getSourceAsMap().fields.rating}"
        }
    }


    def "les films réalisés par Clint Eastwood, en affichant d’abord ceux dans lesquels il joue"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)

        MatchQueryBuilder mqb1 = QueryBuilders.matchQuery("fields.directors", "Clint Eastwood")
        mqb1.operator(Operator.AND)

        MatchQueryBuilder mqb2 = QueryBuilders.matchQuery("fields.actors", "Clint Eastwood")
        mqb2.operator(Operator.AND)
        mqb2.boost(4.0)

        searchSourceBuilder.query(QueryBuilders.boolQuery().must(mqb1).should(mqb2))

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.getSourceAsMap().fields.title} ,${hit.getSourceAsMap().fields.directors}, ${hit.getSourceAsMap().fields.actors}"
        }
    }

    def "les films de Sergio Leone, en les ordonnant du plus récent au plus ancien"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)

        MatchQueryBuilder mqb1 = QueryBuilders.matchQuery("fields.directors", "Sergio Leone")
        mqb1.operator(Operator.AND)
        searchSourceBuilder.sort("fields.year", SortOrder.DESC)

        searchSourceBuilder.query(mqb1)

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.getSourceAsMap().fields.title}, ${hit.getSourceAsMap().fields.directors}, ${hit.getSourceAsMap().fields.year}"
        }
    }
}
