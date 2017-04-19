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

import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeXORRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawErasureCoderFactoryLegacy;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.XORRawErasureCoderFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * This class registers all coder implementation.
 */
public final class CodecRegistry {

  private static CodecRegistry instance = new CodecRegistry();

  public static CodecRegistry getInstance() {
    return instance;
  }

  private Map<String, List<RawErasureCoderFactory>> coderMap;

  private CodecRegistry() {
    coderMap = new HashMap<String, List<RawErasureCoderFactory>>();
    final ServiceLoader<RawErasureCoderFactory> coderFactories =
        ServiceLoader.load(RawErasureCoderFactory.class);
    for (RawErasureCoderFactory coderFactory : coderFactories) {
      String codecName = coderFactory.getCodecName();
      List<RawErasureCoderFactory> coders = coderMap.get(codecName);
      if (coders == null) {
        coders = new ArrayList<RawErasureCoderFactory>();
        coders.add(coderFactory);
        coderMap.put(codecName, coders);
      } else {
        // put ISAL factories first, JAVA factories second
        if (coderFactory instanceof NativeRSRawErasureCoderFactory ||
            coderFactory instanceof NativeXORRawErasureCoderFactory) {
          coders.add(0, coderFactory);
        } else if (coderFactory instanceof RSRawErasureCoderFactory ||
            coderFactory instanceof XORRawErasureCoderFactory ||
            coderFactory instanceof RSRawErasureCoderFactoryLegacy) {
          RawErasureCoderFactory firstCoderFactory = coders.get(0);
          if (firstCoderFactory instanceof NativeRSRawErasureCoderFactory ||
              firstCoderFactory instanceof NativeXORRawErasureCoderFactory) {
            coders.add(1, coderFactory);
          } else {
            coders.add(0, coderFactory);
          }
        } else {
          coders.add(coderFactory);
        }
      }
    }
  }

  /**
   * Get all coder names of the given codec.
   * @param codecName the name of codec
   * @return a list of all coder names
   */
  public String[] getCoderNames(String codecName) {
    List<RawErasureCoderFactory> coders = coderMap.get(codecName);
    if (coders == null) {
      throw new RuntimeException("No available raw coder factory for "
          + codecName);
    }
    List<String> coderNames = new ArrayList<String>();
    for (RawErasureCoderFactory coder : coders) {
      coderNames.add(coder.getCoderName());
    }
    return coderNames.toArray(new String[0]);
  }

  /**
   * Get all coder factories of the given codec.
   * @param codecName the name of codec
   * @return a list of all coder factories
   */
  public List<RawErasureCoderFactory> getCoders(String codecName) {
    List<RawErasureCoderFactory> coders = coderMap.get(codecName);
    if (coders == null) {
      throw new RuntimeException("No available raw coder factory for "
          + codecName);
    }
    return coders;
  }

  /**
   * Get all codec names.
   * @return a set of all codec names
   */
  public Set<String> getCodecs() {
    return coderMap.keySet();
  }

  /**
   * Get a specific coder factory defined by code name and coder name.
   * @param codecName name of the codec
   * @param coderName name of the coder
   * @return the specific coder
   */
  public RawErasureCoderFactory getCoderByCoderName(
      String codecName, String coderName) {
    List<RawErasureCoderFactory> coders = getCoders(codecName);

    // find the RawErasureCoderFactory with the name of coderName
    for (RawErasureCoderFactory coder : coders) {
      if (coder.getCoderName().equals(coderName)) {
        return coder;
      }
    }

    // if not found, throw exception
    throw new RuntimeException("No implementation for the given coder "
        + coderName);
  }
}
