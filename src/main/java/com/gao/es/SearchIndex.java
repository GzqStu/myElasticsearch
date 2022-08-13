package com.gao.es;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;

/**
 * @author ：gaozhiqi
 * @date ：2022/8/13 16:08
 */
public class SearchIndex {
    //es客户端
    private static TransportClient client;
    private static final String index = "index-es";
    private static final String type = "article";

    static {
        init();
    }

    /**
     * 初始化客户端
     */
    public static void init() {
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        //2.创建一个客户端client对象
        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                    .addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302))
                    .addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * es公共查询方法
     *
     * @param queryBuilder
     */
    private void search(QueryBuilder queryBuilder) {
        //创建一个client对象
        //使用client执行查询
        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type).setQuery(queryBuilder).get();
        //得到查询的结果
        SearchHits searchHits = searchResponse.getHits();
        //取查询结果的总记录数
        System.out.println("查询es的数据总数为: " + searchHits.getTotalHits());
        //取查询结果列表
        Iterator<SearchHit> hits = searchHits.iterator();
        while (hits.hasNext()) {
            SearchHit searchHit = hits.next();
            System.out.println("打印结果: " + searchHit.getSourceAsString());
        }
        //关闭client
        client.close();
    }

    /**
     * es公共查询方法---分页
     *
     * @param queryBuilder --查询条件
     * @param from         --起始页
     * @param size         --查询条数
     */
    private void searchByPage(QueryBuilder queryBuilder, Integer from, Integer size) {
        //创建一个client对象
        //使用client执行查询
        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type).setQuery(queryBuilder).setFrom(from).setSize(size).get();
        //得到查询的结果
        SearchHits searchHits = searchResponse.getHits();
        //取查询结果的总记录数
        System.out.println("查询es的数据总数为: " + searchHits.getTotalHits());
        //取查询结果列表
        Iterator<SearchHit> hits = searchHits.iterator();
        while (hits.hasNext()) {
            SearchHit searchHit = hits.next();
            System.out.println("打印结果: " + searchHit.getSourceAsString());
        }
        //关闭client
        client.close();
    }

    /**
     * 根据id来查询es内容
     */
    public void searchById() {
        //创建一个client对象
        //创建一个查询对象，可以使用QueryBuilders工具类创建QueryBuilder对象
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("1", "2");
        search(queryBuilder);
    }

    /**
     * 根据关键词来查询
     */
    public void searchByTerm() {
        //创建一个查询对象，可以使用QueryBuilders工具类创建QueryBuilder对象
        QueryBuilder queryBuilder = QueryBuilders.termQuery("title", "es");
        search(queryBuilder);
    }

    /**
     * 根据queryString查询
     */
    public void searchByQueryString() {
        //创建一个查询对象，可以使用QueryBuilders工具类创建QueryBuilder对象
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("测试向大海走去").defaultField("title");
        search(queryBuilder);
    }

    /**
     * 分页查询
     *
     * @param from       --起始页
     * @param size--查询条数
     */
    public void searchByPage(Integer from, Integer size) {
        //创建一个查询对象，可以使用QueryBuilders工具类创建QueryBuilder对象
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("测试向大海走去").defaultField("title");
        searchByPage(queryBuilder, from, size);
    }

    /**
     * es公共查询方法---设置高亮
     *
     * @param queryBuilder --查询条件
     * @param highlightField --高亮字段
     * @param from --起始页
     * @param size --条数
     */
    private void searchByHighlight(QueryBuilder queryBuilder,String highlightField,Integer from,Integer size) {
        //设置高亮显示字段
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field(highlightField);
        highlightBuilder.preTags("<em>");
        highlightBuilder.postTags("</em>");
        //使用client执行查询
        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type).setQuery(queryBuilder)
                .setFrom(from).setSize(size)
                .highlighter(highlightBuilder).get();
        //得到查询的结果
        SearchHits searchHits = searchResponse.getHits();
        //取查询结果的总记录数
        System.out.println("查询es的数据总数为: " + searchHits.getTotalHits());
        //取查询结果列表
        Iterator<SearchHit> hits = searchHits.iterator();
        while (hits.hasNext()) {
            SearchHit searchHit = hits.next();
            System.out.println("打印结果: " + searchHit.getSourceAsString());
            Map<String, HighlightField > highlightFieldMap = searchHit.getHighlightFields();
            System.out.println("取es中高亮的结果:===="+highlightFieldMap);
            HighlightField field = highlightFieldMap.get(highlightField);
            Text[] texts = field.getFragments();
            if(null!=texts){
                String result = texts[0].toString();
                System.out.println("输出高亮结果: "+result);
            }
        }
        //关闭client
        client.close();
    }

    /**
     * 查询高亮
     * @param highlightField----高亮字段
     * @param from------起始页
     * @param size-----查询条数
     */
    public void searchByHighlight(String highlightField,Integer from,Integer size){
        //创建一个查询对象，可以使用QueryBuilders工具类创建QueryBuilder对象
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("测试向大海走去").defaultField("title");
        searchByHighlight(queryBuilder,highlightField, from, size);
    }
    public static void main(String[] args) {
        SearchIndex searchIndex = new SearchIndex();
        //searchIndex.searchById();
        //searchIndex.searchByTerm();
        //searchIndex.searchByQueryString();
        //searchIndex.searchByPage(60,5);
        searchIndex.searchByHighlight("title",60,5);
    }
}
