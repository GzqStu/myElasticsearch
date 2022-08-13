package com.gao.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * 使用客户端创建es索引库
 *
 * @author ：gaozhiqi
 * @date ：2022/8/13 13:19
 */
public class EsClient {
    //es客户端
    private static TransportClient client;

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
            client = new PreBuiltTransportClient(settings).addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                    .addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302))
                    .addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建es客户端
     *
     * @return
     */
    private TransportClient getEsClient() {
        //1.创建一个settings对象，相当于是一个配置信息，主要是配置集群信息
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        //2.创建一个客户端client对象
        TransportClient client = new PreBuiltTransportClient(settings);
        try {
            client.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
            client.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
            client.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }

    /**
     * 创建es索引库
     */
    public void createEsIndex() {
        //使用客户端client对象创建索引库
        client.admin().indices().prepareCreate("index-es").get();
        //关闭客户端client对象
        client.close();
    }

    /**
     * 创建es mappings
     */
    public void createEsMapping() {
        //创建一个mappings信息，应该是一个json数据，可以是字符串，也可以是XContextBuilder对象
        try {
            //拼接一个mapping信息
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("article")
                    .startObject("properties")
                    .startObject("id").field("type", "long").field("store", true).endObject()
                    .startObject("title").field("type", "text").field("store", true).field("analyzer", "ik_smart").endObject()
                    .startObject("content").field("type", "text").field("store", true).field("analyzer", "ik_smart").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            //创建索引mapping
            client.admin().indices().preparePutMapping("index-es").setType("article").setSource(builder).get();
            //关闭client对象
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过xContentBuild文档格式向索引库中添加文档
     */
    public void addDocumentByXmlContentBuild() {
        try {
            //拼接文档信息
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("id", 1)
                    .field("title", "我用测试es")
                    .field("content", "我非常喜欢es")
                    .endObject();
            //添加文档
            client.prepareIndex().setIndex("index-es").setType("article").setId("1").setSource(builder).get();
            //关闭客户端
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 向索引库中添加文档-json格式
     */
    public void addDocumentByObject() {
        try {
            //创建一个对象
            Article article = new Article(2l, "我的测试大海是多么的蓝", "错误的");
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonDocument = objectMapper.writeValueAsString(article);
            //添加文档
            client.prepareIndex().setIndex("index-es").setType("article").setId("2").setSource(jsonDocument, XContentType.JSON).get();
            //关闭客户端
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void batchAddDocument(){
        try {
            for (int i = 3; i < 200; i++) {
                //创建一个对象
                Article article = new Article(Long.valueOf(i), "我的测试大海是多么的蓝"+i, "错误的西安则了啊是的把好的"+i);
                ObjectMapper objectMapper = new ObjectMapper();
                String jsonDocument = objectMapper.writeValueAsString(article);
                //添加文档
                client.prepareIndex().setIndex("index-es").setType("article").setId(i+"").setSource(jsonDocument, XContentType.JSON).get();
            }
            //关闭客户端
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        EsClient esClient = new EsClient();
        //创建Es索引库
        //esClient.createEsIndex();
        //创建索引库mapping信息
        //esClient.createEsMapping();
        //向索引库中添加文档
        //esClient.addDocumentByXmlContentBuild();
        esClient.batchAddDocument();
    }
}
