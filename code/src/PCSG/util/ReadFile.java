package PCSG.util;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReadFile {

    public static List<List<Integer>> readInteger(String filePath, String regex) {
        List<List<Integer>> result = new ArrayList<>();
        File file = new File(filePath);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                String tokens[] = tempString.split(regex);
                ArrayList<Integer> line = new ArrayList<>();
                for (String iter: tokens) {
                    line.add(Integer.parseInt(iter));
                }
                result.add(line);
            }
            reader.close();
        } catch (IOException e) {
            System.out.println("No File!");
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }

    public static List<List<Double>> readDouble(String filePath, String regex) {
        List<List<Double>> result = new ArrayList<>();
        File file = new File(filePath);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                String tokens[] = tempString.split(regex);
                List<Double> line = new ArrayList<>();
                for (String iter: tokens) {
                    line.add(Double.parseDouble(iter));
                }
                result.add(line);
            }
            reader.close();
        } catch (IOException e) {
            System.out.println("No File!");
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }

    public static List<Integer> readInteger(String filePath) {
        List<Integer> result = new ArrayList<>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(filePath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                result.add(Integer.parseInt(line));
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static List<String> readLine(String filePath, String regex) {
        String line = "";
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))){
            line = reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<String> result = new ArrayList<>(Arrays.asList(line.split(regex)));
        return result;
    }

    public static List<String> readString(String filePath) {
        List<String> result = new ArrayList<>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(filePath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                result.add(line);
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static List<List<String>> readString(String filePath, String regex) {
        List<List<String>> result = new ArrayList<>();
        File file = new File(filePath);
        try(BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
//                System.out.println(tempString);
                String[] tokens = tempString.split(regex);
                ArrayList<String> line = new ArrayList<>(Arrays.asList(tokens));
                result.add(line);
            }
        } catch (IOException e) {
            System.out.println("No File!");
        }
        return result;
    }

    public static boolean compareFiles(String file1, String file2) {
        List<String> content1 = readString(file1);
        List<String> content2 = readString(file2);
        if (content1.size() != content2.size()) {
            return false;
        }
        for (int i = 0; i < content1.size(); i ++) {
            if (!content1.get(i).equals(content2.get(i))) {
                System.out.println(" line " + i + " are dintinct. ");
                return false;
            }
        }
        return true;
    }

}
