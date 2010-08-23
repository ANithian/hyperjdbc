/**
 * Copyright (C) 2008  Luke Lu (Zvents, Inc.)
 *
 * This file is distributed under the Apache Software License
 * (http://www.apache.org/licenses/)
 */

package org.hokiesuns.hypertable;

import java.util.Iterator;
import java.util.List;

import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.MutateSpec;
import org.hypertable.thriftgen.ScanSpec;
import org.hypertable.thriftgen.Schema;

public class BasicClientTest {
  public static void main(String [] args) {
    try {
      ThriftClient client = ThriftClient.create("localhost", 38080);

      // HQL examples
      show(client.hql_query("show tables").toString());
      show(client.hql_query("select * from thrift_test").toString());
      // Schema example
      Schema schema = new Schema();
      schema = client.get_schema("thrift_test");

      Iterator ag_it = schema.access_groups.keySet().iterator();
      show("Access groups:");
      while (ag_it.hasNext()) {
        show("\t" + ag_it.next());
      }

      Iterator cf_it = schema.column_families.keySet().iterator();
      show("Column families:");
      while (cf_it.hasNext()) {
        show("\t" + cf_it.next());
      }

      // mutator examples
      long mutator = client.open_mutator("thrift_test", 0, 0);

      try {
        Cell cell = new Cell();
        cell.key.row = "java-k1";
        cell.key.column_family = "col";
        String vtmp = "java-v1";
        cell.value = vtmp.getBytes();
        client.set_cell(mutator, cell);
      }
      finally {
        client.close_mutator(mutator, true);
      }

      // shared mutator example
      {
        MutateSpec mutate_spec = new MutateSpec();
        mutate_spec.setAppname("test-java");
        mutate_spec.setFlush_interval(1000);
        Cell cell = new Cell();

        cell.key.row = "java-put1";
        cell.key.column_family = "col";
        String vtmp = "java-put-v1";
        cell.value = vtmp.getBytes();
        client.put_cell("thrift_test", mutate_spec, cell);

        cell.key.row = "java-put2";
        cell.key.column_family = "col";
        vtmp = "java-put-v2";
        cell.value = vtmp.getBytes();
        client.refresh_shared_mutator("thrift_test", mutate_spec);
        client.put_cell("thrift_test", mutate_spec, cell);
        Thread.sleep(2000);
      }

      // scanner examples
      ScanSpec scanSpec = new ScanSpec(); // empty scan spec select all
      long scanner = client.open_scanner("thrift_test", scanSpec, true);

      try {
        List<Cell> cells = client.next_cells(scanner);

        while (cells.size() > 0) {
          show(cells.toString());
          cells = client.next_cells(scanner);
        }
      }
      finally {
        client.close_scanner(scanner);
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void show(String line) {
    System.out.println(line);
  }
}