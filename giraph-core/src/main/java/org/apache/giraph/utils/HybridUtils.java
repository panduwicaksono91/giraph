package org.apache.giraph.utils;

import javafx.concurrent.Worker;
import org.apache.giraph.partition.BasicPartitionOwner;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.partition.PartitionStats;
import org.apache.giraph.worker.WorkerInfo;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Utility class for hybrid recovery.
 * @author Pandu Wicaksono
 */
public class HybridUtils {

  /**
   * No constructing this utility class
   */
  private HybridUtils() { }

  /**
   * Print the PartitionOwner
   * @author Pandu
   */
  public static void printPartitionOwners(Collection<PartitionOwner> partitionOwnerColletion){
    System.out.println("printPartitionOwners");
    for(PartitionOwner owner : partitionOwnerColletion) {
      System.out.println("partitionId: " + owner.getPartitionId() + " " + owner.getWorkerInfo().getHostnameId());
    }
  }

  /**
   * Print the PartitionOwner to file
   * @author Pandu
   */
  public static void printPartitionOwnersToFile(Collection<PartitionOwner> partitionOwnerCollection, String homeDir,
                                          String filename){
    String fullFilename = homeDir + "/" + filename;

    java.nio.file.Path p = Paths.get(fullFilename);
    if(Files.exists(p)){
      return;
    }

    try {
      PrintWriter writer = new PrintWriter(fullFilename, "UTF-8");
      for(PartitionOwner owner : partitionOwnerCollection){
        // write the partition id
        writer.write(owner.getPartitionId() + "\n"); // 0

        // write the worker info
        WorkerInfo workerInfo = owner.getWorkerInfo();
        writer.write(workerInfo.getHostname() + "\n"); // hostname 1
        writer.write(workerInfo.getPort() + "\n"); // port 2
        writer.write(workerInfo.getTaskId() + "\n"); // taskID 3
        writer.write(workerInfo.getHostOrIp() + "\n"); // hostOrIp 4

        writer.flush();
      }
      writer.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }

  /**
   * Read PartitionOwner from file
   * @author Pandu Wicaksono
   * @param homeDir
   * @param filename
   * @return Collection of PartitionOwner in the file
   */
  public static Collection<PartitionOwner> readPartitionOwnerFromFile(String homeDir, String filename){
    String file = homeDir + "/" + filename;
    ArrayList<PartitionOwner> result = new ArrayList<PartitionOwner>();

    try {
      BufferedReader br = new BufferedReader(new FileReader(file));
      int counter = 0;
      String partitionID = "";
      String hostname = "";
      String port = "";
      String taskID = "";
      String hostOrIp = "";

      String line = br.readLine();

      while(line != null){
        if(counter == 0) { partitionID = line; }
        if(counter == 1) { hostname = line; }
        if(counter == 2) { port = line;}
        if(counter == 3) { taskID = line; }
        if(counter == 4) { hostOrIp = line; }
        counter++;

        if(counter == 5) { // successfully read the data
          WorkerInfo workerInfo = new WorkerInfo();
          workerInfo.setHostname(hostname);
          workerInfo.setPort(Integer.parseInt(port));
          workerInfo.setTaskId(Integer.parseInt(taskID));
          workerInfo.setHostOrIp(hostOrIp);

          PartitionOwner owner = new BasicPartitionOwner(Integer.parseInt(partitionID), workerInfo);
          result.add(owner);
          counter = 0;
        }

        line = br.readLine();
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e){
      e.printStackTrace();
    }

    return result;
  }

  /**
   * Print WorkerInfo
   * @author Pandu Wicaksono
   * @param workers
   */
  public static void printWorkerInfoList(List<WorkerInfo> workers){
    System.out.println("printWorkerInfoList");

    Collections.sort(workers, new Comparator<WorkerInfo>() {
      @Override
      public int compare(WorkerInfo o1, WorkerInfo o2) {
        return o1.getTaskId() - o2.getTaskId();
      }
    });

    for(WorkerInfo worker : workers){
      System.out.println(worker);
    }
  }

  public static void printWorkerInfoListToFile(List<WorkerInfo> workers, String homeDir, String filename){
    String fullFilename = homeDir + "/" + filename;

    java.nio.file.Path p = Paths.get(fullFilename);
    if(Files.exists(p)){
      return;
    }

    try {
      PrintWriter writer = new PrintWriter(fullFilename, "UTF-8");
      for(WorkerInfo workerInfo : workers){
        // write the worker info
        writer.write(workerInfo.getHostname() + "\n"); // hostname 0
        writer.write(workerInfo.getPort() + "\n"); // port 1
        writer.write(workerInfo.getTaskId() + "\n"); // taskID 2
        writer.write(workerInfo.getHostOrIp() + "\n"); // hostOrIp 3

        writer.flush();
      }
      writer.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }

  /**
   * @author Pandu Wicaksono
   * @param filename
   * @return List of WorkerInfo from file
   */
  public static List<WorkerInfo> readWorkerInfoListFromFile(String homeDir, String filename){
    String file = homeDir + "/" + filename;
    ArrayList<WorkerInfo> result = new ArrayList<WorkerInfo>();

    // if file not found, return null
    java.nio.file.Path p = Paths.get(file);
    if(!Files.exists(p)){
      return null;
    }

    try {
      BufferedReader br = new BufferedReader(new FileReader(file));
      int counter = 0;
      String hostname = "";
      String port = "";
      String taskID = "";
      String hostOrIp = "";

      String line = br.readLine();

      while(line != null){
        if(counter == 0) { hostname = line; }
        if(counter == 1) { port = line;}
        if(counter == 2) { taskID = line; }
        if(counter == 3) { hostOrIp = line; }
        counter++;

        if(counter == 4) { // successfully read the data
          WorkerInfo workerInfo = new WorkerInfo();
          workerInfo.setHostname(hostname);
          workerInfo.setPort(Integer.parseInt(port));
          workerInfo.setTaskId(Integer.parseInt(taskID));
          workerInfo.setHostOrIp(hostOrIp);

          result.add(workerInfo);
          counter = 0;
        }

        line = br.readLine();
      }
      br.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e){
      e.printStackTrace();
    }

    System.out.println("readWorkerInfoListFromFile");
    printWorkerInfoList(result);

    return result;
  }

  /**
   * Notify optimistic recovery mode
   * @param homeDir
   * @param missingWorker
   */
  public static void notifyNettyClient(String homeDir, WorkerInfo missingWorker){
//    public static void notifyNettyClient(String homeDir){
    String fullFilename = homeDir + "/optimistic_signal.txt";

    try {
      PrintWriter writer = new PrintWriter(fullFilename, "UTF-8");
      writer.write("1\n");

      // print the missing workerInfo
      // write the worker info
      writer.write(missingWorker.getHostname() + "\n"); // hostname 0
      writer.write(missingWorker.getPort() + "\n"); // port 1
      writer.write(missingWorker.getTaskId() + "\n"); // taskID 2
      writer.write(missingWorker.getHostOrIp() + "\n"); // hostOrIp 3

      writer.flush();
      writer.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }

  /**
   * Reset the optimistic recovery notification
   * @param homeDir
   */
  public static void resetNotificationNettyClient(String homeDir){
    String fullFilename = homeDir + "/optimistic_signal.txt";

    try {
      PrintWriter writer = new PrintWriter(fullFilename, "UTF-8");
      writer.write("0\n");
      writer.flush();
      writer.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }

  }

  /**
   * delete files necessary for optimistic recovery
   * @param homeDir
   */
  public static void deleteOptimisticFile(String homeDir){
//    LOG.info("deleteOptimisticFile: starts");
    String dir = homeDir;
    String optimistic_signaltxt = "/optimistic_signal.txt";
    String workerInfoListtxt = "/workerInfoList.txt";
    String newWorkertxt = "/newWorker.txt";


    File file = new File(dir + optimistic_signaltxt);
//    LOG.info("deleteOptimisticFile: " + file.toPath() + " flag "  + Files.exists(file.toPath()));
    file.delete();

    file = new File(dir + workerInfoListtxt);
//    LOG.info("deleteOptimisticFile: " + file.toPath() + " flag "  + Files.exists(file.toPath()));
    file.delete();

    file = new File(dir + newWorkertxt);
//    LOG.info("deleteOptimisticFile: " + file.toPath() + " flag "  + Files.exists(file.toPath()));
    file.delete();


    deleteAllFilesInDirectory(homeDir, "/optimistic_dir/");
//    deleteAllFilesInDirectory(homeDir, "/partitionStats_dir/");
    deleteAllFilesInDirectory(homeDir, "/checkpoint_dir/");

//    String optimistic_dir = homeDir + "/optimistic_dir/";
//    File optimistic_directory = new File(optimistic_dir);
//
////    LOG.info("deleteOptimisticFile: for loop delete file");
//    for(File tmpFile: optimistic_directory.listFiles()) {
////      LOG.info("deleteOptimisticFile: try to delete file " + tmpFile.toPath());
//      if (!tmpFile.isDirectory()) {
//        tmpFile.delete();
////        LOG.info("deleteOptimisticFile: file deleted " + tmpFile.toPath());
//      }
//    }
//
//    String partitionStats_dir = homeDir + "/partitionStats_dir/";
//    File partitionStats_directory = new File(partitionStats_dir);
//
////    LOG.info("deleteOptimisticFile: for loop delete file");
//    for(File tmpFile: partitionStats_directory.listFiles()) {
////      LOG.info("deleteOptimisticFile: try to delete file " + tmpFile.toPath());
//      if (!tmpFile.isDirectory()) {
//        tmpFile.delete();
////        LOG.info("deleteOptimisticFile: file deleted " + tmpFile.toPath());
//      }
//    }
  }

  /**
   * Get signal to use optimistic recovery mode
   * @param homeDir
   * @return flag to enter optimistic recovery mode
   */
  public static boolean getOptimisticNotification(String homeDir){

    boolean result = false;
    String file = homeDir + "/optimistic_signal.txt";

    java.nio.file.Path p = Paths.get(file);
    if(!Files.exists(p)){
      return false;
    }

    try {
      BufferedReader br = new BufferedReader(new FileReader(file));
      String line = br.readLine();

      result = (Integer.parseInt(line) == 1) ? true : false;

      br.close();

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e){
      e.printStackTrace();
    }

    return result;
  }

  /**
   * Read the PartitionStats from file
   * @param homeDir
   * @return List of PartitionStats from the file
   */
  public static List<PartitionStats> readPartitionStatsListFromFile(String homeDir){
    List<PartitionStats> result = new ArrayList<PartitionStats>();

    String partitionStats_dir = homeDir + "/partitionStats_dir/";
    File partitionStats_directory = new File(partitionStats_dir);

    for(File tmpFile: partitionStats_directory.listFiles()) {
      // iterate the file
      // add into List
      try {
        BufferedReader br = new BufferedReader(new FileReader(tmpFile));
        int counter = 0;
        String partitionId = "";
        String vertexCount = "";
        String finishedVertexCount = "";
        String edgeCount = "";
        String messagesSentCount = "";
        String messageBytesSentCount = "";
        String computeMs = "";
        String workerHostnameId = "";

        String line = br.readLine();

        while(line != null){
          if(counter == 0) { partitionId = line; }
          if(counter == 1) { vertexCount = line;}
          if(counter == 2) { finishedVertexCount = line; }
          if(counter == 3) { edgeCount = line; }
          if(counter == 4) { messagesSentCount = line; }
          if(counter == 5) { messageBytesSentCount = line; }
          if(counter == 6) { computeMs = line; }
          if(counter == 7) { workerHostnameId = line; }
          counter++;

          if(counter == 8) { // successfully read the data
            PartitionStats stats = new PartitionStats(
                    Integer.parseInt(partitionId),
                    Long.parseLong(vertexCount),
                    Long.parseLong(finishedVertexCount),
                    Long.parseLong(edgeCount),
                    Long.parseLong(messagesSentCount),
                    Long.parseLong(messageBytesSentCount),
                    workerHostnameId);

            stats.setComputeMs(Long.parseLong(computeMs));

            result.add(stats);
            counter = 0;
          }

          line = br.readLine();
        }
        br.close();
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e){
        e.printStackTrace();
      }
    }

    System.out.println("readPartitionStatsListFromFile");
//    printPartitionStatList(result);

    return result;
  }

  /**
   * Print List of PartitionStats
   * @param stats
   */
  public static void printPartitionStatList(List<PartitionStats> stats){
    System.out.println("printPartitionStatsList");

    Collections.sort(stats, new Comparator<PartitionStats>() {
      @Override
      public int compare(PartitionStats o1, PartitionStats o2) {
        return o1.getPartitionId() - o2.getPartitionId();
      }
    });

    for(PartitionStats stat : stats){
      System.out.println(stat.toString());
    }
  }

  public static void printPartitionStatsListToFile(List<PartitionStats> statsList,
                                                   String homeDir, WorkerInfo worker){
    String fullFilename = homeDir + "/partitionStats_dir/"
            + worker.getTaskId() + ".txt";

//    Collections.sort(statsList, new Comparator<PartitionStats>() {
//      @Override
//      public int compare(PartitionStats o1, PartitionStats o2) {
//        return o1.getPartitionId() - o2.getPartitionId();
//      }
//    });

    java.nio.file.Path p = Paths.get(fullFilename);
    if(Files.exists(p)){
      return;
    }

    try {
      PrintWriter writer = new PrintWriter(fullFilename, "UTF-8");
      for(PartitionStats stat : statsList){
        // write the PartitionStats
        writer.write( stat.getPartitionId() + "\n"); // partitionId 0
        writer.write( stat.getVertexCount() + "\n"); // vertexCount 1
        writer.write( stat.getFinishedVertexCount() + "\n"); // finishedVertexCount 2
        writer.write( stat.getEdgeCount() + "\n"); // edgeCount 3
        writer.write( stat.getMessagesSentCount() + "\n"); // messagesSentCount 4
        writer.write( stat.getMessageBytesSentCount() + "\n"); // messageBytesSentCount 5
        writer.write( stat.getComputeMs() + "\n"); // computeMs 6
        writer.write( stat.getWorkerHostnameId() + "\n"); // workerHostnameId 7
        writer.flush();
      }
      writer.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }

  public static WorkerInfo getMissingWorker(String homeDir){
    WorkerInfo missingWorker = null;

    String file = homeDir + "/optimistic_signal.txt";

    java.nio.file.Path p = Paths.get(file);
    if(!Files.exists(p)){
      return null;
    }

    try {
      BufferedReader br = new BufferedReader(new FileReader(file));
      int counter = 0;
      String hostname = "";
      String port = "";
      String taskID = "";
      String hostOrIp = "";

      String line = br.readLine(); // first one is flag
      line = br.readLine();

      while(line != null){
        if(counter == 0) { hostname = line; }
        if(counter == 1) { port = line;}
        if(counter == 2) { taskID = line; }
        if(counter == 3) { hostOrIp = line; }
        counter++;

        if(counter == 4) { // successfully read the data
          WorkerInfo workerInfo = new WorkerInfo();
          workerInfo.setHostname(hostname);
          workerInfo.setPort(Integer.parseInt(port));
          workerInfo.setTaskId(Integer.parseInt(taskID));
          workerInfo.setHostOrIp(hostOrIp);

          missingWorker = workerInfo;
          counter = 0;
        }

        line = br.readLine();
      }

      br.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e){
      e.printStackTrace();
    }

    return missingWorker;
  }

  public static void printCheckpointSuccess(String homeDir, WorkerInfo worker){
    String fullFilename = homeDir + "/checkpoint_dir/" + worker.getTaskId() + ".txt";

    java.nio.file.Path p = Paths.get(fullFilename);
    if(Files.exists(p)){
      return;
    }

    try {
      PrintWriter writer = new PrintWriter(fullFilename, "UTF-8");

      // write anything
      writer.write("1" + "\n");
      writer.flush();
      writer.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }

  public static boolean checkCheckpointFinished(String homeDir, WorkerInfo worker){
    boolean result = false;
    String fullFilename = homeDir + "/checkpoint_dir/" + worker.getTaskId() + ".txt";

    java.nio.file.Path p = Paths.get(fullFilename);
    if(Files.exists(p)){
      result = true;
    }

    return result;
  }

  public static void deleteAllFilesInDirectory(String homeDir, String dir){
    String fullDirectoryName = homeDir + dir;

    File fullDirectory = new File(fullDirectoryName);

    for(File tmpFile: fullDirectory.listFiles()) {
      if (!tmpFile.isDirectory()) {
        tmpFile.delete();
      }
    }
  }

  public static void markKillingProcess(String homeDir, int superstep){
    String fullFilename = homeDir + "/superstepkill_dir/" + superstep + ".txt";

    java.nio.file.Path p = Paths.get(fullFilename);
    if(Files.exists(p)){
      return;
    }

    try {
      PrintWriter writer = new PrintWriter(fullFilename, "UTF-8");

      // write anything
      writer.write("1" + "\n");
      writer.flush();
      writer.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }

  public static boolean checkKillingProcess(String homeDir, int superstep){
    boolean result = false;
    String fullFilename = homeDir + "/superstepkill_dir/" + superstep + ".txt";

    java.nio.file.Path p = Paths.get(fullFilename);
    if(Files.exists(p)){
      result = true;
    }

    return result;
  }

  public static boolean barrierOnHybridFolder(String homeDir, String dir, int numOfFiles){
    boolean result = false;

    String fullDirectoryName = homeDir + dir;

    File fullDirectory = new File(fullDirectoryName);

    if(fullDirectory.listFiles().length == numOfFiles){
      result = true;
    }

    return result;
  }
}
