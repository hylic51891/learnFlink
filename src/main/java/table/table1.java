package table;

import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.runtime.typeutils.ArrayDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;

import java.sql.Array;

public class table1 {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        LogicalType elementType = new IntType();
        ArrayDataSerializer ads = new ArrayDataSerializer(elementType);
        int[] array = new int[]{1,2,3,4,5,6};
    }
}
