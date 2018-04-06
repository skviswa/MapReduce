Following are the instructions to build run and test the program:

1. The package for this program is secsort. The class MeanTempWithYr contains the main method
2. In Eclipse IDE, add a project with the given pom.xml, and create a project named secsort.
3. Add the java programs in the src folder to the created package secsort.
4. Right click on pom.xml, give Run as--> Maven build. 
5. In the goals tab, give clean install. Apply changes and give Run.
6. This will generate a jar file in the target folder of your project.
7. Alternatively, download the target folder as well to skip step 2-6.
7. The makefile has a target make alone, for local testing of the program.
8. It takes input from the input folder and saves result to output folder (same dirctory as makefile) 
9. Add the input directory in the present directory and add the input file to this input folder
10. Ensure the paths in the makefile(for example hadoop location and other paths) correspond to your system. 
11. Then give make alone. Output is stored in the output folder.
   
