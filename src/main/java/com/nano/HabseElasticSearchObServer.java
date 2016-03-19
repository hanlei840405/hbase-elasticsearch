package com.nano;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2016/3/18.
 */
public class HabseElasticSearchObServer extends BaseRegionObserver {

    private Client client;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "es").build();
        client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("elasticsearch"), 9300));
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        GetResponse getResponse = client.prepareGet("hbase", "row-key", Bytes.toString(get.getRow())).get();
        if (getResponse.isExists()) {
            Map<String, ?> hashKey = getResponse.getSource();
            String rowKey = hashKey.get("row-key").toString();
            get = new Get(Bytes.toBytes(rowKey));
        }
        super.preGetOp(e, get, results);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        String hashKey = MD5Hash.getMD5AsHex(put.getRow());
        client.prepareIndex("hbase", "row-key", Bytes.toString(put.getRow())).setSource(Bytes.toString(put.getRow()), hashKey).get();
        super.postPut(e, put, edit, durability);
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        GetResponse getResponse = client.prepareGet("hbase", "row-key", Bytes.toString(delete.getRow())).get();
        if (getResponse.isExists()) {
            client.prepareDelete("hbase", "row-key", Bytes.toString(delete.getRow())).get();
            Map<String, ?> hashKey = getResponse.getSource();
            String rowKey = hashKey.get("row-key").toString();
            delete = new Delete(Bytes.toBytes(rowKey));
        }
        super.postDelete(e, delete, edit, durability);
    }
}
