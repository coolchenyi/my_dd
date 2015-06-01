/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.data;

import com.google.common.primitives.Ints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 */
public class ByteBufferSerializer<T>
{
  public static <T> T read(ByteBuffer buffer, ObjectStrategy<T> strategy)
  {
    int size = buffer.getInt();
    ByteBuffer bufferToUse = buffer.asReadOnlyBuffer();
    bufferToUse.limit(bufferToUse.position() + size);
    buffer.position(bufferToUse.limit());

    return strategy.fromByteBuffer(bufferToUse, size);
  }

  public static <T> void writeToChannel(T obj, ObjectStrategy<T> strategy, WritableByteChannel channel)
      throws IOException
  {
    byte[] toWrite = strategy.toBytes(obj);
    channel.write(ByteBuffer.allocate(Ints.BYTES).putInt(0, toWrite.length));
    channel.write(ByteBuffer.wrap(toWrite));
  }
}
