package jp.co.dk.hbase.sample;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseSample {
	public static void main(String args[]) {
		org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost");
		
		// ＩＮＳＥＲＴ
		try {
			HTable table = new HTable(conf, "tbl");
			
			byte[] fam = Bytes.toBytes("fam");
			byte[] row = Bytes.toBytes("row");
			byte[] col1 = Bytes.toBytes("col1");
			byte[] val1 = Bytes.toBytes("val1");
			byte[] col2 = Bytes.toBytes("col2");
			byte[] val2 = Bytes.toBytes("val2");
			// Putオブジェクトを生成する際にRowKeyを指定
			Put put = new Put(row);
			// PutするColumnをaddする
			put.add(fam, col1, val1);
			put.add(fam, col2, val2);
			
			// Timestamp指定でPutすることも可能です。
			long ts = System.currentTimeMillis();
			put.add(fam, col1, ts, val1);
			put.add(fam, col2, ts, val2);
			
			// Putする
			table.put(put);
			
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// SELECT
		try {
			HTable table = new HTable(conf, "tbl");
		
			byte[] fam = Bytes.toBytes("fam");
			byte[] row = Bytes.toBytes("row");
			byte[] col1 = Bytes.toBytes("col1");
			byte[] col2 = Bytes.toBytes("col2");
	
			// Getオブジェクトを生成する際にRowKeyを指定
			Get get = new Get(row);
			// データをGetする
			Result result = table.get(get);
			System.out.println(Bytes.toString(result.getValue(fam, col1))); // val1
			System.out.println(Bytes.toString(result.getValue(fam, col2))); // val2
			
			table.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// ＤＥＬＥＴＥ
		
		try {
			HTable table = new HTable(conf, "tbl");
			byte[] row = Bytes.toBytes("row");
			Delete delete = new Delete(row);
			table.delete(delete);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
