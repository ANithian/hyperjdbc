package org.hokiesuns.hypertable;

import java.util.List;

import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.HqlResult;

public class HypertableTester 
{
	public static void main(String[] args) throws Exception
	{
		ThriftClient client = ThriftClient.create("192.168.116.128", 38080);
		List<String> tables = client.get_tables();
		System.out.println("Available Tables:");
		for(String s:tables)
		{
			System.out.println(s);
		}
		HqlResult result = client.hql_query("select * from user_table");
		List<Cell> cells = result.cells;
		for(Cell c:cells)
		{
			System.out.println("Key = " + c.getKey() + ",Value = " + new String(c.value));
		}
//		result = client.hql_query("insert into foo values('003','c1','Hello')");
//		System.out.println(result.results);
//		System.out.println(result.cells);
	}
}
