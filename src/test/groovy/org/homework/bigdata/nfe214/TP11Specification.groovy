package org.homework.bigdata.nfe214

import org.apache.http.HttpHost
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.index.query.*
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.range.Range
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.metrics.avg.Avg
import org.elasticsearch.search.aggregations.metrics.max.Max
import org.elasticsearch.search.aggregations.metrics.min.Min
import org.elasticsearch.search.builder.SearchSourceBuilder
import spock.lang.Shared
import spock.lang.Specification

// docker run -d --name remy-elastic -p 9200:9200 -p 9300:9300 elasticsearch
class TP11Specification extends Specification {

    @Shared
    RestHighLevelClient client

    def setupSpec() {
        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200, "http"),
                new HttpHost("localhost", 9300, "http")).build()

        client = new RestHighLevelClient(restClient)
    }

    private SearchRequest buildQueryString(String query) {
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchSourceBuilder.query(QueryBuilders.queryStringQuery(query))
        searchRequest.source(searchSourceBuilder)
        return searchRequest
    }

    def "Recherche des films dont le titre contient life"() {
        when:
        SearchResponse searchResponse = client.search(buildQueryString("fields.title:life"))

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.getSourceAsMap().fields.title} ,${hit.getSourceAsMap().fields.year}"

        }
    }

    def "Les films dont on parle d'une meurtier"() {
        when:
        SearchResponse searchResponse = client.search(buildQueryString("+fields.plot:murder"))

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.getSourceAsMap().fields.title} -> ${hit.getSourceAsMap().fields.plot}"

        }
    }

    def "Les films dont on parle d'une meurtier et de féroce"() {
        when:
        SearchResponse searchResponse = client.search(buildQueryString("+fields.plot:murder,fierce"))

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.getSourceAsMap().fields.title} -> ${hit.getSourceAsMap().fields.plot}"

        }
    }

    def "Les films avec Kate Winslett et Leonardo di Caprio"() {
        when:
        SearchResponse searchResponse = client.search(buildQueryString("fields.actors:Kate Winslett, Leonardo di Caprio"))

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.score} - ${hit.getSourceAsMap().fields.title} -> ${hit.getSourceAsMap().fields.actors}"

        }
    }

    def "Utilisation de query string pour la recherche des films matrix dont l'année est obligatoirement 1999"() {
        when:
        SearchResponse searchResponse = client.search(buildQueryString("+fields.year:1999 AND fields.title:matrix"))

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.getSourceAsMap().fields.title} ,${hit.getSourceAsMap().fields.year}"

        }
    }

    // curl -XGET http://localhost:9200/movies/_search?q=fields.title:Star+Wars | jq
    // curl -XGET http://localhost:9200/movies/_search?q=title:alien,coppola | jq
    def "recherche des films avec le titre Star Wars"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)


        MatchQueryBuilder mqb = QueryBuilders.matchQuery("fields.title", "Star Wars")
        mqb.operator(Operator.AND)
        searchSourceBuilder.query(mqb)

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.score} - ${hit.getSourceAsMap().fields.title} ,${hit.getSourceAsMap().fields.year}"
        }
    }

    def "Films Star Wars dont le réalisateur est Georges Lucas"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)


        MatchQueryBuilder mqbStarWars = QueryBuilders.matchQuery("fields.title", "Star Wars")
        mqbStarWars.operator(Operator.AND)

        MatchQueryBuilder mqbGeorges = QueryBuilders.matchQuery("fields.directors", "George Lucas")
        mqbGeorges.operator(Operator.AND)

        BoolQueryBuilder booleanQuery = QueryBuilders.boolQuery().must(mqbStarWars).must(mqbGeorges)

        searchSourceBuilder.query(booleanQuery)

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.score} - ${hit.getSourceAsMap().fields.title}, ${hit.getSourceAsMap().fields.directors}"
        }
    }

    def "Films dans lesquels Harrison Ford a joué"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)


        MatchQueryBuilder mqb = QueryBuilders.matchQuery("fields.actors", "Harrison Ford")
        mqb.operator(Operator.AND)

        searchSourceBuilder.query(mqb)

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.score} - ${hit.getSourceAsMap().fields.title}, ${hit.getSourceAsMap().fields.actors}"
        }
    }

    def "Films dans lesquels Harrison Ford a joué et dont le résumé contient Jones"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)


        MatchQueryBuilder mqb = QueryBuilders.matchQuery("fields.actors", "Harrison Ford")
        mqb.operator(Operator.AND)

        MatchQueryBuilder mqbJones = QueryBuilders.matchQuery("fields.plot", "Jones")

        BoolQueryBuilder booleanQuery = QueryBuilders.boolQuery().must(mqb).must(mqbJones)

        searchSourceBuilder.query(booleanQuery)

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.score} - ${hit.getSourceAsMap().fields.title}, ${hit.getSourceAsMap().fields.actors}, ${hit.getSourceAsMap().fields.plot}"
        }
    }

    def "Films dans lesquels Harrison Ford a joué et dont le résumé contient Jones mais sans le mot Nazi"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)


        MatchQueryBuilder mqb = QueryBuilders.matchQuery("fields.actors", "Harrison Ford")
        mqb.operator(Operator.AND)

        MatchQueryBuilder mqbJones = QueryBuilders.matchQuery("fields.plot", "Jones")

        MatchQueryBuilder mqbNazi = QueryBuilders.matchQuery("fields.plot", "Nazis")

        BoolQueryBuilder booleanQuery = QueryBuilders.boolQuery().must(mqb).must(mqbJones).mustNot(mqbNazi)

        searchSourceBuilder.query(booleanQuery)

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.score} - ${hit.getSourceAsMap().fields.title}, ${hit.getSourceAsMap().fields.actors}, ${hit.getSourceAsMap().fields.plot}"
        }
    }

    def "Films de James Cameron dont le rang devrait être inférieur à 1000"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)

        MatchQueryBuilder mqb = QueryBuilders.matchQuery("fields.directors", "James Cameron")
        mqb.operator(Operator.AND)

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("fields.rank")
        rangeQueryBuilder.lt(1000)

        BoolQueryBuilder booleanQuery = QueryBuilders.boolQuery().must(mqb).must(rangeQueryBuilder)

        searchSourceBuilder.query(booleanQuery)

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.score} - ${hit.getSourceAsMap().fields.title}, ${hit.getSourceAsMap().fields.rank}"
        }
    }

    def "Nombre de films par année"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)

        searchSourceBuilder.aggregation(AggregationBuilders.terms("byYear").field("fields.year"))

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        Terms byYear = searchResponse.getAggregations().get("byYear")
        byYear.getBuckets().each { Terms.Bucket bucket ->
            println "${bucket.key} - ${bucket.docCount}"
        }
    }

    def "Donner la note moyenne des films"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)

        searchSourceBuilder.aggregation(AggregationBuilders.avg("avgRating").field("fields.rating"))

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        Avg avgRating = searchResponse.getAggregations().get("avgRating")
        println avgRating.getValue()
    }

    def "Donner la note moyenne et le rang moyen des films de George Lucas"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)

        MatchQueryBuilder queryB = QueryBuilders.matchQuery("fields.directors", "George Lucas")
        queryB.operator(Operator.AND)
        searchSourceBuilder.query(queryB)

        AggregationBuilder avgRatingAggrBuilder = AggregationBuilders.avg("avgRating").field("fields.rating")
        AggregationBuilder avgRankAggrBuilder = AggregationBuilders.avg("avgRank").field("fields.rank")

        searchSourceBuilder.aggregation(avgRatingAggrBuilder)
        searchSourceBuilder.aggregation(avgRankAggrBuilder)

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        searchResponse.getHits().each { SearchHit hit ->
            println "${hit.getSourceAsMap().fields.rank}/${hit.getSourceAsMap().fields.rating} - ${hit.getSourceAsMap().fields.title}, ${hit.getSourceAsMap().fields.directors}"
        }
        Avg avgRating = searchResponse.getAggregations().get("avgRating")
        println "AVG_RATING : ${avgRating.getValue()}"
        Avg avgRank = searchResponse.getAggregations().get("avgRank")
        println "AVG_RANK : ${avgRank.getValue()}"
    }

    def "Donner la note moyenne des films par année"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)

        searchSourceBuilder.aggregation(AggregationBuilders.terms("byYear").field("fields.year")
                .subAggregation(AggregationBuilders.avg("avgRating").field("fields.rating")))

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        Terms byYear = searchResponse.getAggregations().get("byYear")
        byYear.getBuckets().each { Terms.Bucket bucket ->
            Avg avgRating = bucket.getAggregations().get("avgRating")
            println "Year : ${bucket.key} - Nb Films : ${bucket.docCount} - AvgRating : ${avgRating.value}"
        }
    }

    def "Donner la note minimum, maximum et moyenne des films par année"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)

        searchSourceBuilder.aggregation(AggregationBuilders.terms("byYear").field("fields.year")
                .subAggregation(AggregationBuilders.avg("avgRating").field("fields.rating"))
                .subAggregation(AggregationBuilders.min("minRating").field("fields.rating"))
                .subAggregation(AggregationBuilders.max("maxRating").field("fields.rating")))

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        Terms byYear = searchResponse.getAggregations().get("byYear")
        byYear.getBuckets().each { Terms.Bucket bucket ->
            Avg avgRating = bucket.getAggregations().get("avgRating")
            Min minRating = bucket.getAggregations().get("minRating")
            Max maxRating = bucket.getAggregations().get("maxRating")
            println "Year : ${bucket.key} - Nb Films : ${bucket.docCount} - AvgRating : ${avgRating.value} - MinRating : ${minRating.value} - MaxRating : ${maxRating.value}"
        }
    }

    def "Donner le rang moyen des films par année et trier par ordre décroissant"() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)

        searchSourceBuilder.aggregation(AggregationBuilders.terms("byYear").field("fields.year").order(Terms.Order.term(false))
                .subAggregation(AggregationBuilders.avg("avgRank").field("fields.rank")))

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        Terms byYear = searchResponse.getAggregations().get("byYear")
        byYear.getBuckets().each { Terms.Bucket bucket ->
            Avg avgRank = bucket.getAggregations().get("avgRank")
            println "Year : ${bucket.key} - Nb Films : ${bucket.docCount} - AvgRank : ${avgRank.value}"
        }
    }

    def "Compter le nombre de films par tranche ...-1.9 2-3.9 4-5.9 6-7.9 8-+..."() {
        setup:
        SearchRequest searchRequest = new SearchRequest("movies")
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchRequest.source(searchSourceBuilder)

        searchSourceBuilder.aggregation(AggregationBuilders.range("filmRange").field("fields.rating")
                .addUnboundedTo(1.9)
                .addRange(2,3.9)
                .addRange(4,5.9)
                .addRange(6,7)
                .addUnboundedFrom(8)
        )

        when:
        SearchResponse searchResponse = client.search(searchRequest)

        then:
        Range filmRange = searchResponse.getAggregations().get("filmRange")
        filmRange.getBuckets().each { Range.Bucket bucket ->
            println "${bucket.from} ... ${bucket.to} : ${bucket.docCount}"
        }
    }
}
