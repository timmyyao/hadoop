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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeXORRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureCoderFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class registers all coder implementations.
 *
 * {@link CodecRegistry} maps codec names to coder factories. All coder
 * factories are dynamically identified and loaded using ServiceLoader.
 */
@InterfaceAudience.Private
public final class CodecRegistry {

  private static final Log LOG = LogFactory.getLog(CodecRegistry.class);

  private static CodecRegistry instance = new CodecRegistry();

  public static CodecRegistry getInstance() {
    return instance;
  }

  private Map<String, List<RawErasureCoderFactory>> coderMap;

  private CodecRegistry() {
    coderMap = new HashMap<>();
    final ServiceLoader<RawErasureCoderFactory> coderFactories =
        ServiceLoader.load(RawErasureCoderFactory.class);
    for (RawErasureCoderFactory coderFactory : coderFactories) {
      String codecName = coderFactory.getCodecName();
      List<RawErasureCoderFactory> coders = coderMap.get(codecName);
      if (coders == null) {
        coders = new ArrayList<>();
        coders.add(coderFactory);
        coderMap.put(codecName, coders);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Codec registered: codec = " + coderFactory.getCodecName()
              + ", coder = " + coderFactory.getCoderName());
        }
      } else {
        Boolean hasConflit = false;
        for (RawErasureCoderFactory coder : coders) {
          if (coder.getCoderName().equals(coderFactory.getCoderName())) {
            hasConflit = true;
            LOG.error("Coder " + coderFactory.getClass().getName() +
                " cannot be registered because its coder name " + coderFactory
                + " has conflict with " + coder.getClass().getName());
            break;
          }
        }
        if (!hasConflit) {
          // set native coders as default if user does not
          // specify a fallback order
          if (coderFactory instanceof NativeRSRawErasureCoderFactory ||
                  coderFactory instanceof NativeXORRawErasureCoderFactory) {
            coders.add(0, coderFactory);
          } else {
            coders.add(coderFactory);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Codec registered: codec = " + coderFactory.getCodecName()
                + ", coder = " + coderFactory.getCoderName());
          }
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
      throw new IllegalArgumentException("No available raw coder factory for "
          + codecName);
    }
    List<String> coderNames = coders.stream().
        map(RawErasureCoderFactory::getCoderName).collect(Collectors.toList());
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
      throw new IllegalArgumentException("No available raw coder factory for "
          + codecName);
    }
    return coders;
  }

  /**
   * Get all codec names.
   * @return a set of all codec names
   */
  public Set<String> getCodecNames() {
    return coderMap.keySet();
  }

  /**
   * Get a specific coder factory defined by codec name and coder name.
   * @param codecName name of the codec
   * @param coderName name of the coder
   * @return the specific coder
   */
  public RawErasureCoderFactory getCoderByName(
      String codecName, String coderName) {
    List<RawErasureCoderFactory> coders = getCoders(codecName);

    // find the RawErasureCoderFactory with the name of coderName
    for (RawErasureCoderFactory coder : coders) {
      if (coder.getCoderName().equals(coderName)) {
        return coder;
      }
    }

    // if not found, throw exception
    throw new IllegalArgumentException("No implementation for coder "
        + coderName + " of codec " + codecName);
  }
}
