README Lab1 Arol Garcia Rodriguez, Oscar Colom Pascual de Riquelme, Javier González Otero

### COMPARISONS BETWEEN DIFFERENT LANGUAGES AND DIFFERENT NUMBER OF FILES

1 file -> Eurovision9
Size: 374,490 KB

#### Language Statistics

| Language | Time (ms) | Output Weight (kB) | Proportion of Tweets (%) |
|----------|-----------|--------------------|--------------------------|
| es       | 34,424    | 177,195            | 47.178                   |
| ca       | 22,577    | 1,210              | 0.3                      |
| en       | 30,046    | 108,507            | 28.974                   |


3 files -> (Eurovision9, Eurovision3, Eurovision8)
Combined Size: 1,315,569 KB

#### Language Statistics

| Language | Time (ms) | Output Weight (kB) | Proportion of Tweets (%) |
|----------|-----------|--------------------|--------------------------|
| es       | 128,516   | 572,476            | 43.51                    |
| ca       | 70,485    | 4,649              | 0.353                    |
| en       | 102,149   | 400,419            | 30.4                     |


5 files -> (Eurovision9, Eurovision3, Eurovision8, Eurovision5, Eurovision7)
Combined size: 2,571,850 KB
#### Language Statistics

| Language | Time (ms) | Output Weight (kB) | Proportion of Tweets (%) |
|----------|-----------|--------------------|--------------------------|
| es       | 298,127   | 1,204,416          | 46.83                    |
| ca       | 123,867   | 9,284              | 0.361                    |
| en       | 260,906   | 823,169            | 32.00                    |

As we can appreciate, Spanish is the most commonly used language to tweet about Eurovision, which contradicts the intuition that English is more used.
Also, we can see that Catalan is practically not used, with just a 0,338% on average. We thought that it would be more used.


### RUNTIME ENVIRONMENT DESCRIPTION
Eurovision Processing Application
Hardware:
Laptop with Intel Core i7 processor (8 cores)
RAM: 16GB

Software:
Operating System: Ubuntu 20.04 LTS
Programming Language: Java
Project management tool: Maven

Cloud Service:
AWS

Storage:
Data stored in AWS S3 bucket

Time Unit:
Milliseconds


We didn't found any problem when performing neither the executions nor the calculations.

### TO RUN THE APPLICATION
java -cp target/lab1-1.0-SNAPSHOT.jar edu.upf.TwitterFilter <idiom code> <output destination path with output filename included> lsds2024.lab1.output.u188236 <Input files to process SAVED LOCALLY>

WE HAVE USED OSCAR COLOM PASCUAL DE RIQUELME'S U188236 to store the output files in AWS.
