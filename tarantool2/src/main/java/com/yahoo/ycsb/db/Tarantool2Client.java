/**
 * Copyright (c) 2014 - 2016 YCSB Contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.sopovs.moradanen.tarantool.Result;
import com.sopovs.moradanen.tarantool.TarantoolClient;
import com.sopovs.moradanen.tarantool.TarantoolClientImpl;
import com.sopovs.moradanen.tarantool.core.Iter;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

/**
 * YCSB binding for <a href="http://tarantool.org/">Tarantool</a>.
 */
public class Tarantool2Client extends DB {
  private static final String HOST_PROPERTY = "tarantool.host";
  private static final String PORT_PROPERTY = "tarantool.port";
  private static final String SPACE_PROPERTY = "tarantool.space";
  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_PORT = "3303";
  private static final String DEFAULT_SPACE = "1024";

  private TarantoolClient client;
  private int spaceNo;

  public void init() throws DBException {
    Properties props = getProperties();

    int port = Integer.parseInt(props.getProperty(PORT_PROPERTY, DEFAULT_PORT));
    String host = props.getProperty(HOST_PROPERTY, DEFAULT_HOST);
    spaceNo = Integer.parseInt(props.getProperty(SPACE_PROPERTY, DEFAULT_SPACE));

    try {
      this.client = new TarantoolClientImpl(host, port);
    } catch (Exception exc) {
      throw new DBException("Can't initialize Tarantool connection", exc);
    }
  }

  public void cleanup() throws DBException {
    this.client.close();
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return replace(key, values, "Can't insert element");
  }

  private <T extends Map<String, ByteIterator>> T readRow(Result tResult, Set<String> fields, T mapResult) {
    for (int i = 1; i < tResult.currentSize(); i += 2) {
      String field = tResult.getString(i);
      if (fields == null || fields.contains(field)) {
        mapResult.put(field, new ByteArrayByteIterator(tResult.getBytes(i + 1)));
      }
    }
    return mapResult;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      client.select(this.spaceNo, 0);
      client.setString(key);
      Result tResult = client.execute();
      if (tResult.next()) {
        readRow(tResult, fields, result);
      }
      if (tResult.hasNext()) {
        System.err.println("Multiple result for unique select");
        return Status.ERROR;
      }
      return Status.OK;
    } catch (Exception exc) {
      System.err.println("Can't select element");
      exc.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    try {
      client.select(spaceNo, 0, recordcount, 0, Iter.GE);
      client.setString(startkey);
      Result tResult = client.execute();
      while (tResult.next()) {
        result.add(readRow(tResult, fields, new HashMap<String, ByteIterator>()));
      }
    } catch (Exception exc) {
      System.err.println("Can't select range elements");
      exc.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    try {
      client.delete(spaceNo, 0);
      client.setString(key);
      client.executeUpdate();
    } catch (Exception exc) {
      System.err.println("Can't delete element");
      exc.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {

    return replace(key, values, "Can't replace element");
  }

  private Status replace(String key, Map<String, ByteIterator> values, String exceptionDescription) {
    try {
      client.replace(spaceNo);
      client.setString(key);

      for (Map.Entry<String, ByteIterator> i : values.entrySet()) {
        client.setString(i.getKey());
        client.setBytes(i.getValue().toArray());
      }
      client.execute().consume();

    } catch (Exception exc) {
      System.err.println(exceptionDescription);
      exc.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;

  }
}
