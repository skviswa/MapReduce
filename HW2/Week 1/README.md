Following are the instructions to build run and test the program:

1. There are 3 package for 3 programs (combiner, no combiner, in mapper combiner).
2. The packages are meantemp, meantempcombiner, meantempinmapcomb respectively.
3. The class MeanTemp, MeanTemp2 and MeanTempInMapComb contain the main method for each of the above package respectively.
4. In Eclipse IDE, add a project with the given pom.xml, and create a project named meantemp and create the packages.
5. Add the java programs in the src folder to the created packages mentioned above.
6. Right click on pom.xml, give Run as--> Maven build. 
7. In the goals tab, give clean install. Apply changes and give Run.
8. This will generate a jar file in the target folder of your project.
9. Alternatively, download the target folder as well to skip step 4-8.
10. The makefile has a target make alone, for local testing of the program.
11. It takes input from the input folder and saves result to 3 output folders output, output1 and output2 (same dirctory as makefile) 
12. Add the input directory in the present directory and add the input file to this input folder
13. Ensure the paths in the makefile(for example hadoop location and other paths) correspond to your system. 
14. Then give make alone. Output is stored in the output folder.
   
