// SPDX-License-Identifier: Apache-2.0
import java.nio.file.*;
import java.util.*;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.*;
import org.apache.pinot.spi.data.*;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.JsonUtils;
public class Generate {
  public static void main(String[] args) throws Exception {
    var root=Path.of(args[0]); Files.createDirectories(root.resolve("native")); Files.createDirectories(root.resolve("tables"));
    for (String table : List.of("fact","customerDim","productDim")) {
      var schema = new Schema.SchemaBuilder().setSchemaName(table).setEnableColumnBasedNullHandling(true)
        .addSingleValueDimension("k1",FieldSpec.DataType.INT).addSingleValueDimension("k2",FieldSpec.DataType.INT)
        .addMetric("amount",FieldSpec.DataType.INT).addDateTime("ts",FieldSpec.DataType.LONG,"1:MILLISECONDS:EPOCH","1:HOURS").build();
      var config = new TableConfigBuilder(TableType.OFFLINE).setTableName(table).setTimeColumnName("ts")
        .setServerTenant(table.equals("fact")?"Fact":"Dim").setBrokerTenant("DefaultTenant").build();
      Files.writeString(root.resolve("tables/"+table+"-schema.json"),JsonUtils.objectToPrettyString(schema));
      Files.writeString(root.resolve("tables/"+table+"-table.json"),JsonUtils.objectToPrettyString(config));
      for(int seg=0; seg<(table.equals("fact")?16:1); seg++){
        List<GenericRow> rows=new ArrayList<>();
        for(int j=0; j<(table.equals("fact")?32768:table.equals("customerDim")?4096:1024);j++){
          int i=seg*32768+j;
          var row=new GenericRow();
          row.putValue("k1",table.equals("fact")?i%4096:i);
          row.putValue("k2",table.equals("fact")?(i*17+i/4096)%1024:i%(table.equals("customerDim")?16:8));
          row.putValue("amount",table.equals("fact")?1+i%101:0); row.putValue("ts",1700000000000L); rows.add(row);
        }
        var gen=new SegmentGeneratorConfig(config,schema); gen.setOutDir(root.resolve("native").toString());
        gen.setTableName(table); gen.setSegmentName(table+"_"+seg);
        var driver=new SegmentIndexCreationDriverImpl();
        try(var r=new GenericRowRecordReader(rows)) {driver.init(gen,r); driver.build();}
      }
    }
  }
}
