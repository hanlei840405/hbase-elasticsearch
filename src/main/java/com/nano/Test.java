package com.nano;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Created by Administrator on 2016/3/19.
 */
public class Test {

    public static void main(String... args) throws UnknownHostException {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "es").build();

        Client client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(
                        "elasticsearch", 9300));
        GetResponse getResponse = client.prepareGet("hbase", "row-key", "aaa").get();
        System.out.println(getResponse.getSource());
    }
}
