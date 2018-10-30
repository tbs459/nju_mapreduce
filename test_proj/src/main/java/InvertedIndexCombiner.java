public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>{
        private Text info = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            //统计词频
            int wordcnt = 0;
            for (Text value : values) {
                wordcnt+= Integer.parseInt(value.toString() );
            }

            int splitIndex = key.toString().indexOf(":");

            //重新设置value值由filename和词频组成
            info.set( key.toString().substring( splitIndex + 1) +":"+wordcnt );

            //重新设置key值为单词
            key.set( key.toString().substring(0,splitIndex));

            context.write(key, info);
        }
    }
