package org.hokiesuns.hypertable;

import java.util.List;

import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.HqlResult;

public class HypertableTester 
{
	public static void main(String[] args) throws Exception
	{
		ThriftClient client = ThriftClient.create("engineering-mm.admin.zvents.com", 38080);
		long lNameSpace=client.open_namespace("/");
		List<String> tables = client.get_tables(lNameSpace);
		System.out.println("Available Tables:");
		for(String s:tables)
		{
			System.out.println(s);
		}
		HqlResult result = client.hql_query(lNameSpace,"select * from user_table");
		List<Cell> cells = result.cells;
		for(Cell c:cells)
		{
			System.out.println("Key = " + c.getKey() + ",Value = " + new String(c.getValue()));
		}
//		result = client.hql_query("insert into foo values('003','c1','Hello')");
//		System.out.println(result.results);
//		System.out.println(result.cells);
	}
}
