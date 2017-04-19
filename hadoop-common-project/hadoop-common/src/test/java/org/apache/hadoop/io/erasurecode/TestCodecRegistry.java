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
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test CodecRegistry.
 */
public class TestCodecRegistry {
  @Test
  public void testGetCodecs() {
    Set<String> codecs = CodecRegistry.getInstance().getCodecs();
    assertEquals(3, codecs.size());
    assertTrue(codecs.contains(ErasureCodeConstants.RS_CODEC_NAME));
    assertTrue(codecs.contains(ErasureCodeConstants.RS_LEGACY_CODEC_NAME));
    assertTrue(codecs.contains(ErasureCodeConstants.XOR_CODEC_NAME));
  }

  @Test
  public void testGetCoders() {
    List<RawErasureCoderFactory> coders = CodecRegistry.getInstance().
        getCoders(ErasureCodeConstants.RS_CODEC_NAME);
    assertEquals(2, coders.size());
    assertTrue(coders.get(0) instanceof NativeRSRawErasureCoderFactory);
    assertTrue(coders.get(1) instanceof RSRawErasureCoderFactory);

    coders = CodecRegistry.getInstance().
        getCoders(ErasureCodeConstants.RS_LEGACY_CODEC_NAME);
    assertEquals(1, coders.size());
    assertTrue(coders.get(0) instanceof RSRawErasureCoderFactoryLegacy);

    coders = CodecRegistry.getInstance().
        getCoders(ErasureCodeConstants.XOR_CODEC_NAME);
    assertEquals(2, coders.size());
    assertTrue(coders.get(0) instanceof NativeXORRawErasureCoderFactory);
    assertTrue(coders.get(1) instanceof XORRawErasureCoderFactory);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCodersWrong() {
    List<RawErasureCoderFactory> coders =
        CodecRegistry.getInstance().getCoders("WRONG_CODEC");
  }

  @Test
  public void testGetCoderNames() {
    String[] coderNames = CodecRegistry.getInstance().
        getCoderNames(ErasureCodeConstants.RS_CODEC_NAME);
    assertEquals(2, coderNames.length);
    assertEquals(NativeRSRawErasureCoderFactory.CODER_NAME,
        coderNames[0]);
    assertEquals(RSRawErasureCoderFactory.CODER_NAME, coderNames[1]);

    coderNames = CodecRegistry.getInstance().
        getCoderNames(ErasureCodeConstants.RS_LEGACY_CODEC_NAME);
    assertEquals(1, coderNames.length);
    assertEquals(RSRawErasureCoderFactoryLegacy.CODER_NAME,
        coderNames[0]);

    coderNames = CodecRegistry.getInstance().
        getCoderNames(ErasureCodeConstants.XOR_CODEC_NAME);
    assertEquals(2, coderNames.length);
    assertEquals(NativeXORRawErasureCoderFactory.CODER_NAME,
        coderNames[0]);
    assertEquals(XORRawErasureCoderFactory.CODER_NAME, coderNames[1]);
  }

  @Test
  public void testGetCoderByName() {
    RawErasureCoderFactory coder = CodecRegistry.getInstance().
            getCoderByName(ErasureCodeConstants.RS_CODEC_NAME,
        RSRawErasureCoderFactory.CODER_NAME);
    assertTrue(coder instanceof RSRawErasureCoderFactory);

    coder = CodecRegistry.getInstance().getCoderByName(
        ErasureCodeConstants.RS_CODEC_NAME,
        NativeRSRawErasureCoderFactory.CODER_NAME);
    assertTrue(coder instanceof NativeRSRawErasureCoderFactory);

    coder = CodecRegistry.getInstance().getCoderByName(
        ErasureCodeConstants.RS_LEGACY_CODEC_NAME,
        RSRawErasureCoderFactoryLegacy.CODER_NAME);
    assertTrue(coder instanceof RSRawErasureCoderFactoryLegacy);

    coder = CodecRegistry.getInstance().getCoderByName(
        ErasureCodeConstants.XOR_CODEC_NAME,
        XORRawErasureCoderFactory.CODER_NAME);
    assertTrue(coder instanceof XORRawErasureCoderFactory);

    coder = CodecRegistry.getInstance().getCoderByName(
        ErasureCodeConstants.XOR_CODEC_NAME,
        NativeXORRawErasureCoderFactory.CODER_NAME);
    assertTrue(coder instanceof NativeXORRawErasureCoderFactory);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetCoderByNameWrong() {
    RawErasureCoderFactory coder = CodecRegistry.getInstance().
            getCoderByName(ErasureCodeConstants.RS_CODEC_NAME, "WRONG_RS");
  }
}
