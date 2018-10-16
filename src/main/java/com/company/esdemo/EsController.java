package com.company.esdemo;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@RestController
public class EsController {

    @Autowired
    private TransportClient client;

    @GetMapping("/")
    public String index() {
        return "index";
    }

    @GetMapping("/get/book/novel")
    @ResponseBody
    public ResponseEntity get(@RequestParam(name = "id", defaultValue = "") String id) {

        System.out.println("begin");

        // prepareGet
        System.out.println("prepareGet");
        GetRequestBuilder getBuilder = client.prepareGet("book", "novel", id);

        // do get
        System.out.println("do get");
        GetResponse getResponse = getBuilder.get();

        if (!getResponse.isExists()) {
            System.out.println("getResponse not exist");
        }

        System.out.println("end");
        return new ResponseEntity(getResponse.getSource(), HttpStatus.OK);
    }

    @PostMapping("add/book/novel")
    @ResponseBody
    public ResponseEntity add(
            @RequestParam(name = "title") String title,
            @RequestParam(name = "author") String author,
            @RequestParam(name = "word_count") Integer wordCount,
            @RequestParam(name = "publish_date") @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date publishDate
    ) throws IOException {

        System.out.println("begin");

        // xContent
        System.out.println("xContent");
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .field("title", title)
                .field("author", author)
                .field("word_count", wordCount)
                .field("publish_date", publishDate.getTime())
                .endObject();

        // prepareIndex
        System.out.println("prepareIndex");
        IndexRequestBuilder indexBuilder = client.prepareIndex("book", "novel")
                .setSource(xContentBuilder);

        // do get
        System.out.println("do get");
        IndexResponse indexResponse = indexBuilder.get();

        System.out.println("end");
        return new ResponseEntity(indexResponse.getId(), HttpStatus.OK);
    }

    @DeleteMapping("delete/book/novel")
    @ResponseBody
    public ResponseEntity delete(@RequestParam(name = "id") String id) {

        System.out.println("begin");

        // prepareDelete
        System.out.println("prepareDelete");
        DeleteRequestBuilder deleteBuilder = client.prepareDelete("book", "novel", id);

        // do get
        System.out.println("do get");
        DeleteResponse deleteResponse = deleteBuilder.get();

        System.out.println("end");
        return new ResponseEntity(deleteResponse.getResult().toString(), HttpStatus.OK);
    }

    @PutMapping("update/book/novel")
    @ResponseBody
    public ResponseEntity update(
            @RequestParam(name = "id") String id,
            @RequestParam(name = "title", required = false) String title,
            @RequestParam(name = "author", required = false) String author
    ) throws IOException, ExecutionException, InterruptedException {

        System.out.println("begin");

        // xContent
        System.out.println("xContent");
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .startObject();
        if (title != null) {
            xContentBuilder.field("title", title);
        }
        if (author != null) {
            xContentBuilder.field("author", author);
        }
        xContentBuilder.endObject();

        // update
        System.out.println("update");
        UpdateRequest updateRequest = new UpdateRequest("book", "novel", id);
        updateRequest.doc(xContentBuilder);

        // set upate and do get
        System.out.println("set upate and do get");
        ActionFuture<UpdateResponse> updateResponseActionFuture = client.update(updateRequest);
        UpdateResponse updateResponse = updateResponseActionFuture.get();

        System.out.println("end");
        return new ResponseEntity(updateResponse.getResult().toString(), HttpStatus.OK);
    }

    @PostMapping("query/book/novel")
    @ResponseBody
    public ResponseEntity query(
            @RequestParam(name = "author", required = false) String author,
            @RequestParam(name = "title", required = false) String title,
            @RequestParam(name = "gt_word_count", defaultValue = "0") Integer gtWordCount,
            @RequestParam(name = "lt_word_count", required = false) Integer ltWordCount
    ) {

        System.out.println("begin");

        // boolBuilder
        System.out.println("boolBuilder");
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        if (author != null) {
            MatchQueryBuilder builder = QueryBuilders.matchQuery("author", author);
            boolBuilder.must(builder);
        }
        if (title != null) {
            MatchQueryBuilder builder = QueryBuilders.matchQuery("title", title);
            boolBuilder.must(builder);
        }

        // rangeBuilder
        System.out.println("rangeBuilder");
        RangeQueryBuilder rangeBuilder = QueryBuilders.rangeQuery("word_count");
        rangeBuilder.from(gtWordCount);
        if (ltWordCount != null && ltWordCount > 0) {
            rangeBuilder.to(ltWordCount);
        }

        // set rangeBuilder
        System.out.println("set rangeBuilder");
        boolBuilder.filter(rangeBuilder);

        // searchBuilder
        System.out.println("searchBuilder");
        SearchRequestBuilder searchBuilder = client.prepareSearch("book");
        searchBuilder.setTypes("novel")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolBuilder)
                .setFrom(0)
                .setSize(10);
        System.out.println(searchBuilder);

        // searchBuilder do get
        System.out.println("searchBuilder do get");
        SearchResponse searchResponse = searchBuilder.get();
        System.out.println(searchResponse);

        // result
        System.out.println("result");
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        for (SearchHit hit : searchResponse.getHits()) {
            result.add(hit.getSourceAsMap());
        }

        System.out.println("end");
        return new ResponseEntity(result, HttpStatus.OK);
    }

}
