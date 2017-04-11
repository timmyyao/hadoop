/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.erasurecode;

import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureCoderFactory;

import java.util.*;

/**
 * This class registers all coder implementation.
 */
public class CoderRegistry {
  public static final String DUMMY_CODEC_NAME = "dummy";

  public static final String IO_ERASURECODE_CODER_NAME_DUMMY = "dummy";
  public static final String IO_ERASURECODE_CODER_NAME_XOR_DEFAULT = "xor_java";
  public static final String IO_ERASURECODE_CODER_NAME_XOR_ISAL = "xor_isal";
  public static final String IO_ERASURECODE_CODER_NAME_RS_DEFAULT = "rs_java";
  public static final String IO_ERASURECODE_CODER_NAME_RS_ISAL = "rs_isal";
  public static final String IO_ERASURECODE_CODER_NAME_RSLEGACY_DEFAULT = "rs-legacy_isal";

  private static CoderRegistry instance = new CoderRegistry();

  public static CoderRegistry getInstance() {
    return instance;
  }

  private Map<String, List<RawErasureCoderFactory>> coderMap;

  private CoderRegistry() {
    coderMap = new HashMap<String, List<RawErasureCoderFactory>>();
    final ServiceLoader<RawErasureCoderFactory> coderFactories = ServiceLoader.load(RawErasureCoderFactory.class);
    for (RawErasureCoderFactory coderFactory : coderFactories) {
      String codecName = coderFactory.getCodecName();
      String coderName = coderFactory.getCoderName();
      List<RawErasureCoderFactory> coders = coderMap.get(codecName);
      if (coders == null) {
        coders = new ArrayList<RawErasureCoderFactory>();
        coders.add(coderFactory);
        coderMap.put(codecName, coders);
      }
      else {
        // put DEFAULT coder in the first
        if (coderName.equals(IO_ERASURECODE_CODER_NAME_XOR_DEFAULT) ||
                coderName.equals(IO_ERASURECODE_CODER_NAME_RS_DEFAULT) ||
                coderName.equals(IO_ERASURECODE_CODER_NAME_RSLEGACY_DEFAULT)) {
          coders.add(0, coderFactory);
        }
        // put ISAL coder after DEFAULT
        else if (coderName.equals(IO_ERASURECODE_CODER_NAME_XOR_ISAL) ||
                coderName.equals(IO_ERASURECODE_CODER_NAME_RS_ISAL)) {
          String firstCoderName = coders.get(0).getCoderName();
          if (firstCoderName.equals(IO_ERASURECODE_CODER_NAME_XOR_DEFAULT) ||
                  firstCoderName.equals(IO_ERASURECODE_CODER_NAME_RS_DEFAULT) ||
                  firstCoderName.equals(IO_ERASURECODE_CODER_NAME_RSLEGACY_DEFAULT)) {
            coders.add(1, coderFactory);
          }
          else {
            coders.add(0, coderFactory);
          }
        }
        else {
          coders.add(coderFactory);
        }
      }
    }
  }

  /**
   * Get all coder implementations of the given codec
   * @param codecName the name of codec
   * @return a list of all coder names
   */
  public List<RawErasureCoderFactory> getCoders(String codecName) {
    List<RawErasureCoderFactory> coders = coderMap.get(codecName);
    if (coders == null) {
      throw new RuntimeException("No available raw coder factory for " + codecName);
    }
    return coders;
  }

  /**
   * Get all codec names
   * @return a set of all codec names
   */
  public Set<String> getCodecs() {
    return coderMap.keySet();
  }

  /**
   * Get a specific coder factory defined by code name and coder name
   * @param codecName name of the codec
   * @param coderName name of the coder
   * @return the specific coder or default
   */
  public RawErasureCoderFactory getCoderByCoderName(String codecName, String coderName) {
    List<RawErasureCoderFactory> coders = getCoders(codecName);

    // find the RawErasureCoderFactory with the name of coderName
    for (RawErasureCoderFactory coder : coders) {
      if (coder.getCoderName().equals(coderName)) {
        return coder;
      }
    }

    // if not found, use default
    return coders.get(0);
  }

  /**
   * Get a specific coder factory according to the order of coder names
   * @param codecName name of the codec
   * @param coderNames a list of coder names
   * @return the specific coder or default
   */
  public RawErasureCoderFactory getCoderByCoderNames(String codecName, String[] coderNames) {
    List<RawErasureCoderFactory> coders = getCoders(codecName);

    // use default order if not defined
    if (coderNames == null || coderNames.length == 0) {
      return coders.get(0);
    }

    // get the coder factory according to the input order
    for (String coderName : coderNames) {
      for (RawErasureCoderFactory coder : coders) {
        if (coder.getCoderName().equals(coderName)) {
          return coder;
        }
      }
    }

    // if not found, use default
    return coders.get(0);
  }
}
