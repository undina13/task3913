package com.javarush.task.task39.task3913;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MyFileVisitor extends SimpleFileVisitor<Path> {
    ArrayList<String> log = new ArrayList<>();
    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

       if(file.getFileName().toString().endsWith(".log")) {


           List<String> lines = Files.readAllLines(file);


           log.addAll(lines);
       }


        return FileVisitResult.CONTINUE;
    }
}
