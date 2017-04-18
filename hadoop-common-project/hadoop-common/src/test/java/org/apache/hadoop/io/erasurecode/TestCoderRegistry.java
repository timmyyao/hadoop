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
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.apache.hadoop.io.erasurecode.CoderRegistry.*;
import static org.apache.hadoop.io.erasurecode.ErasureCodeConstants.RS_CODEC_NAME;
import static org.apache.hadoop.io.erasurecode.ErasureCodeConstants.RS_LEGACY_CODEC_NAME;
import static org.apache.hadoop.io.erasurecode.ErasureCodeConstants.XOR_CODEC_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test CoderRegistry.
 */
public class TestCoderRegistry {
  @Test
  public void testGetCodecs() {
    Set<String> codecs = CoderRegistry.getInstance().getCodecs();
    assertEquals(3, codecs.size());
    assertTrue(codecs.contains(RS_CODEC_NAME));
    assertTrue(codecs.contains(RS_LEGACY_CODEC_NAME));
    assertTrue(codecs.contains(XOR_CODEC_NAME));
  }

  @Test
  public void testGetCoders() {
    List<RawErasureCoderFactory> coders = CoderRegistry.getInstance().getCoders(RS_CODEC_NAME);
    assertEquals(2,coders.size());
    assertEquals(IO_ERASURECODE_CODER_NAME_RS_DEFAULT, coders.get(0).getCoderName());
    assertEquals(IO_ERASURECODE_CODER_NAME_RS_ISAL, coders.get(1).getCoderName());

    coders = CoderRegistry.getInstance().getCoders(RS_LEGACY_CODEC_NAME);
    assertEquals(1, coders.size());
    assertEquals(IO_ERASURECODE_CODER_NAME_RSLEGACY_DEFAULT, coders.get(0).getCoderName());

    coders = CoderRegistry.getInstance().getCoders(XOR_CODEC_NAME);
    assertEquals(2, coders.size());
    assertEquals(IO_ERASURECODE_CODER_NAME_XOR_DEFAULT, coders.get(0).getCoderName());
    assertEquals(IO_ERASURECODE_CODER_NAME_XOR_ISAL, coders.get(1).getCoderName());
  }

  @Test(expected = RuntimeException.class)
  public void testGetCodersWrong() {
    List<RawErasureCoderFactory> coders = CoderRegistry.getInstance().getCoders("WRONG_CODEC");
  }

  @Test
  public void testGetCoderNames() {
    String[] coderNames = CoderRegistry.getInstance().getCoderNames(RS_CODEC_NAME);
    assertEquals(2, coderNames.length);
    assertEquals(IO_ERASURECODE_CODER_NAME_RS_DEFAULT, coderNames[0]);
    assertEquals(IO_ERASURECODE_CODER_NAME_RS_ISAL, coderNames[1]);

    coderNames = CoderRegistry.getInstance().getCoderNames(RS_LEGACY_CODEC_NAME);
    assertEquals(1, coderNames.length);
    assertEquals(IO_ERASURECODE_CODER_NAME_RSLEGACY_DEFAULT, coderNames[0]);

    coderNames = CoderRegistry.getInstance().getCoderNames(XOR_CODEC_NAME);
    assertEquals(2, coderNames.length);
    assertEquals(IO_ERASURECODE_CODER_NAME_XOR_DEFAULT, coderNames[0]);
    assertEquals(IO_ERASURECODE_CODER_NAME_XOR_ISAL, coderNames[1]);
  }

  @Test
  public void testGetCoderByCoderName() {
    RawErasureCoderFactory coder = CoderRegistry.getInstance().getCoderByCoderName(
            RS_CODEC_NAME, IO_ERASURECODE_CODER_NAME_RS_DEFAULT);
    assertEquals(IO_ERASURECODE_CODER_NAME_RS_DEFAULT, coder.getCoderName());

    coder = CoderRegistry.getInstance().getCoderByCoderName(
            RS_CODEC_NAME, IO_ERASURECODE_CODER_NAME_RS_ISAL);
    assertEquals(IO_ERASURECODE_CODER_NAME_RS_ISAL, coder.getCoderName());

    coder = CoderRegistry.getInstance().getCoderByCoderName(
            RS_LEGACY_CODEC_NAME, IO_ERASURECODE_CODER_NAME_RSLEGACY_DEFAULT);
    assertEquals(IO_ERASURECODE_CODER_NAME_RSLEGACY_DEFAULT, coder.getCoderName());

    coder = CoderRegistry.getInstance().getCoderByCoderName(
            XOR_CODEC_NAME, IO_ERASURECODE_CODER_NAME_XOR_DEFAULT);
    assertEquals(IO_ERASURECODE_CODER_NAME_XOR_DEFAULT, coder.getCoderName());

    coder = CoderRegistry.getInstance().getCoderByCoderName(
            XOR_CODEC_NAME, IO_ERASURECODE_CODER_NAME_XOR_ISAL);
    assertEquals(IO_ERASURECODE_CODER_NAME_XOR_ISAL, coder.getCoderName());
  }

  @Test(expected = RuntimeException.class)
  public void testGetCoderByCoderNameWrong() {
    RawErasureCoderFactory coder = CoderRegistry.getInstance().getCoderByCoderName(
            RS_CODEC_NAME, "WRONG_RS");
  }
}
