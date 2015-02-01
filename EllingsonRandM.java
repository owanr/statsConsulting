//Q for program

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.lang.StringBuilder;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

public class EllingsonRandM {
	private static int numOfRainfallRecords = 228;
	private static int numOfS1Records = 1268;
	private static int numOfS2Records = 1549;
	private static int numOfS3Records = 1631;
	private static int numOfS4Records = 1298;
	private static int numOfS5Records = 1510;


    /*
    ** Copies data from file to list of arrays.
    ** Originial data did not contain NA values, so cleaning was unnecessary.
    ** numRows is directly coded in method because there is only one rainfall file.
    */
	private static List<double[]> copyRainfallFileToList(String fileName) {
        //rainfall: year, month, day, precipitation (numCols = 4)
        List<double[]> recordList = new ArrayList<double[]>();

        try {    
            File dataFile = new File(fileName);
            BufferedReader reader = new BufferedReader(new FileReader(dataFile));
            
            String firstLine = reader.readLine();
            for (int i = 0; i < numOfRainfallRecords; i++) {
                String[] fileLine = reader.readLine().replaceAll("\\s+","").split(",");
                
                //Changes date from yyyy/mm/dd to [yyyy,mm,dd]
                String[] yearMonthDate = fileLine[0].split("/");

                double[] oneRow = new double[4];
                //Records date
                oneRow[0] = Double.parseDouble(yearMonthDate[0]);
                oneRow[1] = Double.parseDouble(yearMonthDate[1]);
                oneRow[2] = Double.parseDouble(yearMonthDate[2]);
                
                //Records precipitation data
                oneRow[3] = Double.parseDouble(fileLine[3]);

                //Adds row to list
                recordList.add(oneRow);

                /* Debugging code
                // System.out.println(recordArray[i][0] + "  " + recordArray[i][1] + "  "
                //     + recordArray[i][2] + "  " + recordArray[i][3]);
                */
            }

            
            reader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return recordList;
    }//end of copyRainfallFileToArray

    /*
    ** Copies data from csv files to list of arrays with NA values removed.
    */
    private static List<double[]> copyStationFileToList(String fileName, int rowNum) {
        //station: year, month, day, %M1, %M2, %M3  (numCols = 6)
        List<double[]> recordList = new ArrayList<double[]>();
        int numNA = 0;

        try {    
            File dataFile = new File(fileName);
            BufferedReader reader = new BufferedReader(new FileReader(dataFile));
            
            String firstLine = reader.readLine();
            for (int i = 0; i < rowNum; i++) {
                String[] fileLine = reader.readLine().replaceAll("\\s+","").split(",");

                //Changes date from yyyy/mm/dd to [yyyy,mm,dd]
                String[] yearMonthDate = fileLine[0].split("/");
    			
                //Checks for NA data
                boolean hasNA = false;
                if (yearMonthDate[0].equals("") || yearMonthDate[1].equals("") 
                    || yearMonthDate[2].equals("") || fileLine[1].equals("") || fileLine[8].equals("")
                    || fileLine[9].equals("") || fileLine[10].equals("")
                    ||yearMonthDate[0].equals("#VALUE!") || yearMonthDate[1].equals("#VALUE!") 
                    || yearMonthDate[2].equals("#VALUE!") || fileLine[1].equals("#VALUE!") || fileLine[8].equals("#VALUE!")
                    || fileLine[9].equals("#VALUE!") || fileLine[10].equals("#VALUE!")) {
                    numNA++;
                    hasNA = true;
                }

                if (!hasNA) {

                    double[] oneRow = new double[8];

                    //Records date
                    oneRow[0] = Double.parseDouble(yearMonthDate[2].substring(0,4));
                    oneRow[1] = Double.parseDouble(yearMonthDate[0]);
                    oneRow[2] = Double.parseDouble(yearMonthDate[1]);
                    if (yearMonthDate[2].length() == 8) {
                        oneRow[3] = Double.parseDouble(yearMonthDate[2].replaceAll(":","").substring(4,7));
                    } else {
                        oneRow[3] = Double.parseDouble(yearMonthDate[2].replaceAll(":","").substring(4,8));
                    }
                    oneRow[4] = Double.parseDouble(fileLine[1]);

                    //fileLine[1]-fileLine[7]: number data and sensor/MP data

                    //Records precipitation data--------------------------------
                    
                    oneRow[5] = Double.parseDouble(fileLine[8]);
                    oneRow[6] = Double.parseDouble(fileLine[9]);
                    oneRow[7] = Double.parseDouble(fileLine[10]);

                    recordList.add(oneRow);
                    // Debugging code
                    // System.out.println(recordArray[i][0] + "  " + recordArray[i][1] + "  "
                    //     + recordArray[i][2] + "  " + recordArray[i][3] + "  "
                    //     + recordArray[i][4] + "  " + recordArray[i][5] + "  ");
                }
                
            }
            System.out.println(fileName + " had " + numNA + " rows removed.");
            reader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return recordList;
    }

/*
    ** Copies data from csv files to list of arrays with NA values replaced with average.
    */
    private static List<double[]> copyStationFileToListWithNAAvg(String fileName, int rowNum) {
        //station: year, month, day, %M1, %M2, %M3  (numCols = 6)
        List<double[]> recordList = new ArrayList<double[]>();
        int numNA = 0;
        List<String[]> entireStationData = new ArrayList<String[]>();

        //Copy entire data set into List
        try {    
            File dataFile = new File(fileName);
            BufferedReader reader = new BufferedReader(new FileReader(dataFile));
            reader.readLine(); //Gets rid of header

            for (int i = 0; i < rowNum; i++) {
                String[] fileLine = reader.readLine().replaceAll("\\s+","").split(",");
                String[] entireLine = new String[fileLine.length + 2]; 
                entireLine[0] = fileLine[0].split("/")[0];
                entireLine[1] = fileLine[0].split("/")[1];
                entireLine[2] = fileLine[0].split("/")[2].substring(0,4);
                entireLine[3] = fileLine[0].split("/")[2].substring(4).replace(":","");
                entireLine[4] = fileLine[1]; //time stamp
                entireLine[5] = fileLine[8];
                entireLine[6] = fileLine[9];
                entireLine[7] = fileLine[10];
                //System.arraycopy(fileLine, 1, entireLine, 4, fileLine.length - 2);

                entireStationData.add(entireLine);
                //System.out.println(entireLine[7]);
            }

            reader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        String[] previous = {entireStationData.get(0)[5], entireStationData.get(0)[6], entireStationData.get(0)[7]};

        String[] next = new String[3];
        List tempNextList = Arrays.asList(entireStationData.get(1));
        int n = 1;
        while ((tempNextList.contains("") || tempNextList.contains("#VALUE!")) &&  (n < (entireStationData.size() - 1))) {
            n++;
            tempNextList = Arrays.asList(entireStationData.get(n));
        }
        next[0] = entireStationData.get(n)[5];
        next[1] = entireStationData.get(n)[6];
        next[2] = entireStationData.get(n)[7];
        
        for (int i = 0; i < rowNum; i++) {
            //Debugging
            System.out.println("prev: " + previous[0] + ", " + previous[1] + ", " + previous[2]);
            System.out.println("next: " + next[0] + ", " + next[1] + ", " + next[2]);
            
            List<String> tempList = Arrays.asList(entireStationData.get(i));

            if (tempList.contains("") || tempList.contains("#VALUE!")) {
                System.out.println("row" + i + " contains NA");
                double[] oneRow = new double[8];

                //Records date
                oneRow[0] = Double.parseDouble(tempList.get(2));
                oneRow[1] = Double.parseDouble(tempList.get(0));
                oneRow[2] = Double.parseDouble(tempList.get(1));
                oneRow[3] = Double.parseDouble(tempList.get(3));
                oneRow[4] = Double.parseDouble(tempList.get(4));

                for (int j = 5; j < 8; j++) {

                    //Get value from nearby rows if NA
                    if (tempList.get(j).equals("") || tempList.get(j).equals("#VALUE!")) {

                        try {
                            double tempDouble = (Double.parseDouble(previous[j-5]) + Double.parseDouble(next[j-5]))/2;
                            oneRow[j] = tempDouble;
                            //Don't adjust previous or next

                        } catch (Exception e) {
                            //Only for when the first line is NA
                            if (previous[j-5].equals("") || previous[j-5].equals("#VALUE!")) {

                                oneRow[j] = Double.parseDouble(next[j-5]);
                            }

                            //Only for when the last line is NA
                            if (next[j-5].equals("") || next[j-5].equals("#VALUE!")) {
                                oneRow[j] = Double.parseDouble(previous[j-5]);
                            }
                        }
                    } else {
                        oneRow[j] = Double.parseDouble(tempList.get(j));
                        //Don't adjust previous or next because we should keep sets of values from the same day
                    }
                }

                recordList.add(oneRow);

                
                //If the data other than the moisture data is missing, 
                //Doesn't include that line in record
                //Maybe include count?

            } else {
                //Records data
                double[] oneRow = new double[8];

                //Records date
                oneRow[0] = Double.parseDouble(tempList.get(2));
                oneRow[1] = Double.parseDouble(tempList.get(0));
                oneRow[2] = Double.parseDouble(tempList.get(1));
                oneRow[3] = Double.parseDouble(tempList.get(3));
                oneRow[4] = Double.parseDouble(tempList.get(4));

                //fileLine[1]-fileLine[7]: number data and sensor/MP data

                //Records precipitation data--------------------------------
                oneRow[5] = Double.parseDouble(tempList.get(5));
                oneRow[6] = Double.parseDouble(tempList.get(6));
                oneRow[7] = Double.parseDouble(tempList.get(7));

                recordList.add(oneRow);

                previous[0] = entireStationData.get(i)[5];
                previous[1] = entireStationData.get(i)[6];
                previous[2] = entireStationData.get(i)[7];
                //Re-searches everytime
                //Would be nice if we could just keep next values until i catches up
                //For now, leave like this
                int j = i + 1;
                if (j != entireStationData.size()) {
                    List tempList2 = Arrays.asList(entireStationData.get(j));
                    while ((tempList2.contains("") || tempList2.contains("#VALUE!")) &&  j < (entireStationData.size() - 1)) {
                        j++;
                        tempList2 = Arrays.asList(entireStationData.get(j));
                    }
                    next[0] = entireStationData.get(j)[5];
                    next[1] = entireStationData.get(j)[6];
                    next[2] = entireStationData.get(j)[7];
                }
            }//end of if-else
        }//end of for-loop

        return recordList;
    }

    /*
    ** Gets data for the intersection of rainfall data and station data.
    ** All matrices in parameter have NAs removed.
    */
    private static List<double[]> combineRainfallStation(List<double[]> rainfall, List<double[]> station) {    	
        //Rainfall recorded once a day (3/1 - 10/14)
    	//Stations recorded several times a day (6/25 - 9/14)

        //Combined matrix will never have more rows than station.length

    	int row = station.size();
        System.out.println(row);
        
        List<double[]> combineRainfallStation = new ArrayList<double[]>();

        //int j keeps track of station data index
    	int j = 0;

    	for (int i = 0; i < numOfRainfallRecords; i++) {
            //System.out.println("i goes til end? "+i +" to "+ numOfRainfallRecords);
            //No need to readjust because rainfall is measured everyday

            //Increases station row number if rainfallData is ahead until
            //station row number reaches the end of the station file.
            while (((rainfall.get(i)[1] > station.get(j)[1])
                || ((rainfall.get(i)[1] == station.get(j)[1]) && (rainfall.get(i)[2] > station.get(j)[2])))
                && j < row) {
                //System.out.println(station[j][1]+"/"+station[j][2]+" to "+rainfall[i][1]+"/"+rainfall[i][2]);
                j++;
            }

            //Prevents index error once j == row
            if (j == row) {
                return combineRainfallStation;
            }

            // if (!((rainfall[i][0] == station[j][0]) 
            //     && (rainfall[i][1] == station[j][1]) && (rainfall[i][2] == station[j][2]))) {
            //     System.out.println("RAINNNN "+i);
            //     System.out.println(rainfall[i][0] + " "+  station[j][0]);
            //     System.out.println(rainfall[i][1] + " "+  station[j][1]);
            //     System.out.println(rainfall[i][2] + " "+  station[j][2]);
            //     j++;

            // }

            //Extracts data from the same dates.
            //Repeats rainfall data for multiple station data of the same day
    		while ((rainfall.get(i)[0] == station.get(j)[0]) 
                && (rainfall.get(i)[1] == station.get(j)[1]) && (rainfall.get(i)[2] == station.get(j)[2])) {

    			//System.out.println("rain row: "+i+", station 1 row: "+j);
                double[] oneRow = new double[9];
                
                //Date
                oneRow[0] = rainfall.get(i)[0];
    			oneRow[1] = rainfall.get(i)[1];
    			oneRow[2] = rainfall.get(i)[2];
                oneRow[3] = station.get(j)[3];
                oneRow[4] = station.get(j)[4];
    			//Moisture data
    			oneRow[5] = station.get(j)[5];
    			oneRow[6] = station.get(j)[6];
    			oneRow[7] = station.get(j)[7];
    			//precipation
    			oneRow[8] = rainfall.get(i)[3];

                combineRainfallStation.add(oneRow);

                // System.out.println(combineRainfallStation[realRow][0]+","+combineRainfallStation[realRow][1]+","
                //     +combineRainfallStation[realRow][2]+","+combineRainfallStation[realRow][3]+","+combineRainfallStation[i][4]+","
                //     +combineRainfallStation[realRow][5]+","+combineRainfallStation[realRow][6]);
    			j++;
                if (j == row) {
                    System.out.println("J REACHED MAX");
                    return combineRainfallStation;
                }
    		}
    	}//End of for-loop

    	return combineRainfallStation;
    }

    /*
    ** Gets data for the intersection of rainfall data and station data.
    ** All matrices in parameter have NAs replaced with averages.
    ** In addiion, moisture levels are averaged to match rainfall measurements
    */
    private static List<double[]> combineWithAvgMoisture(List<double[]> rainfall, List<double[]> station) {     
        //Rainfall recorded once a day (3/1 - 10/14)
        //Stations recorded several times a day (6/25 - 9/14)

        //Combined matrix will never have more rows than station.length

        int row = station.size();
        
        List<double[]> combineRainfallStation = new ArrayList<double[]>();

        //int j keeps track of station data index
        int j = 0;

        for (int i = 0; i < numOfRainfallRecords; i++) {

            //Increases station row number if rainfallData is ahead until
            //station row number reaches the end of the station file.
            while (((rainfall.get(i)[1] > station.get(j)[1])
                || ((rainfall.get(i)[1] == station.get(j)[1]) && (rainfall.get(i)[2] > station.get(j)[2])))
                && j < row) {
                //System.out.println(station[j][1]+"/"+station[j][2]+" to "+rainfall[i][1]+"/"+rainfall[i][2]);
                j++;
                //System.out.println("**");
            }

            //Prevents index errors
            if (j == row) {
                return combineRainfallStation;
            }

            double[] oneRow = new double[7];
                
            //Date: Just year, month, day
            oneRow[0] = rainfall.get(i)[0];
            oneRow[1] = rainfall.get(i)[1];
            oneRow[2] = rainfall.get(i)[2];

            //Averages moisture data from the same dates.
            double depth1 = 0.0;
            double depth2 = 0.0;
            double depth3 = 0.0;
            double num = 0.0;

            while ((j < row) && (rainfall.get(i)[0] == station.get(j)[0]) 
                && (rainfall.get(i)[1] == station.get(j)[1]) && (rainfall.get(i)[2] == station.get(j)[2])) {
                
                num++;

                depth1 += station.get(j)[5];
                depth2 += station.get(j)[6];
                depth3 += station.get(j)[7];
                
                j++;
            }
             
            oneRow[3] = depth1/num;
            oneRow[4] = depth2/num;
            oneRow[5] = depth3/num;

            if (depth1 == 0.0) {
                oneRow[3] = 0.0;
            }
            if (depth2 == 0.0) {
                oneRow[4] = 0.0;
            }
            if (depth3 == 0.0) {
                oneRow[5] = 0.0;
            }
            
            //precipation
            oneRow[6] = rainfall.get(i)[3];

            combineRainfallStation.add(oneRow);

            if (j == row) {
                return combineRainfallStation;
            }
        }//End of for-loop

        return combineRainfallStation;
    }

    /*
    ** Takes passed on matrix and copies it into a file of the same name as given String.
    */ 
    private static void copyFileToCSV(String writtenFileName, List<double[]> fileContent, String type) {
        //Writes standardized data into textfile as they are calculated
        try {
            FileWriter writer = new FileWriter(writtenFileName);

            //Makes headers
            writer.append("year");
            writer.append(',');
            writer.append("month");
            writer.append(',');
            writer.append("day");
            writer.append(',');

            //comparison includes time and numberDate because it repeats rainfall data for each moisture measurement
            //comparison 2 uses the average moisture for each day and hence has one row per day
            if (type.equals("station") || type.equals("comparison")) {
                writer.append("time");
                writer.append(',');
                writer.append("numberDate");
                writer.append(',');
            }

            if (type.equals("station") || type.equals("comparison") || type.equals("comparison2")) {
                writer.append("moisture1ft");
                writer.append(',');
                writer.append("moisture2ft");
                writer.append(',');
                writer.append("moisture3ft");
                if (type.equals("comparison") || type.equals("comparison2")) {
                    writer.append(',');
                }
                
            }

            if (type.equals("rainfall") || type.equals("comparison") || type.equals("comparison2")) {
            	writer.append("precipitation");
			}
            writer.append('\n');

            //Inserts content
            int row = fileContent.size();
            int column = fileContent.get(0).length;
            
            for (int i = 0; i < row; i++) {
            	for (int j = 0; j < column - 1; j++) {
            		writer.append((fileContent.get(i))[j]+"");
            		writer.append(',');
            		//System.out.print(fileContent[i][j]);
            	}
            	writer.append((fileContent.get(i))[column - 1]+"");

            	writer.append('\n');
            }

            writer.flush();
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }//end of copyFileToCSV

	public static void main(String[] args) {
		List<double[]> rainfallData = copyRainfallFileToList("EllingsonRainfallAlt.csv");
  //       List<double[]> station1Data = copyStationFileToList("EllingsonJan2015Sheet1Alt.csv", numOfS1Records);
  //       List<double[]> station2Data = copyStationFileToList("EllingsonJan2015Sheet2Alt.csv", numOfS2Records);
  //       List<double[]> station3Data = copyStationFileToList("EllingsonJan2015Sheet3Alt.csv", numOfS3Records);
  //       List<double[]> station4Data = copyStationFileToList("EllingsonJan2015Sheet4Alt.csv", numOfS4Records);
  //       List<double[]> station5Data = copyStationFileToList("EllingsonJan2015Sheet5Alt.csv", numOfS5Records);
        List<double[]> station1Data = copyStationFileToListWithNAAvg("EllingsonJan2015Sheet1Alt.csv", numOfS1Records);
        List<double[]> station2Data = copyStationFileToListWithNAAvg("EllingsonJan2015Sheet2Alt.csv", numOfS2Records);
        List<double[]> station3Data = copyStationFileToListWithNAAvg("EllingsonJan2015Sheet3Alt.csv", numOfS3Records);
        List<double[]> station4Data = copyStationFileToListWithNAAvg("EllingsonJan2015Sheet4Alt.csv", numOfS4Records);
        List<double[]> station5Data = copyStationFileToListWithNAAvg("EllingsonJan2015Sheet5Alt.csv", numOfS5Records);
       
        // List<double[]> comparison1 = combineRainfallStation(rainfallData, station1Data);
        // List<double[]> test1 = combineWithAvgMoisture(rainfallData, station1Data);
        // List<double[]> comparison2 = combineRainfallStation(rainfallData, station2Data);
        // List<double[]> comparison3 = combineRainfallStation(rainfallData, station3Data);
        // List<double[]> comparison4 = combineRainfallStation(rainfallData, station4Data);
        // List<double[]> comparison5 = combineRainfallStation(rainfallData, station5Data);
        List<double[]> comparison1 = combineWithAvgMoisture(rainfallData, station1Data);
        List<double[]> comparison2 = combineWithAvgMoisture(rainfallData, station2Data);
        List<double[]> comparison3 = combineWithAvgMoisture(rainfallData, station3Data);
        List<double[]> comparison4 = combineWithAvgMoisture(rainfallData, station4Data);
        List<double[]> comparison5 = combineWithAvgMoisture(rainfallData, station5Data);

        copyFileToCSV("station1rainfallFile.csv", comparison1, "comparison2");
        copyFileToCSV("station2rainfallFile.csv", comparison2, "comparison2");
        copyFileToCSV("station3rainfallFile.csv", comparison3, "comparison2");
        copyFileToCSV("station4rainfallFile.csv", comparison4, "comparison2");
        copyFileToCSV("station5rainfallFile.csv", comparison5, "comparison2");


	}
}//end of class