public class TriangleCountDriver {
    public static void main(String args[]){
        if (args.length < 3){
            System.out.println("Please input inputPath ,tmpPath and outputPath!");
            return ;
        }

        GraphBuild.main(args);
        System.out.println("end GraphBuild");
        TriangleCount.main(args);
    }
}
