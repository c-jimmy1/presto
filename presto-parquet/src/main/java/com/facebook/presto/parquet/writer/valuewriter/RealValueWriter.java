/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.parquet.writer.valuewriter;

import com.facebook.presto.common.block.Block;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.schema.PrimitiveType;

import java.util.function.Supplier;

import static com.facebook.presto.common.type.RealType.REAL;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

public class RealValueWriter
        extends PrimitiveValueWriter
{
    public RealValueWriter(Supplier<ValuesWriter> valuesWriterSupplier, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriterSupplier);
    }

    @Override
    public void write(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!block.isNull(i)) {
                float value = intBitsToFloat(toIntExact(REAL.getLong(block, i)));
                getValueWriter().writeFloat(value);
                getStatistics().updateStats(value);
            }
        }
    }
}
