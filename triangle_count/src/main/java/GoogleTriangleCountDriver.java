public class GoogleTriangleCountDriver {
    public static void main(String args[]){
        if (args.length < 4){
            System.out.println("Please input inputPath ,tmpPath1, tmpPath2, and outputPath!");
            return ;
        }

        GoogleGetEdge.main(args);
        GoogleGraphBuild.main(args);
        System.out.println("end GraphBuild");
        GoogleTriangleCount.main(args);
    }
}
