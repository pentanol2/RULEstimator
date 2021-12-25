/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.devshop.client;

import com.opencsv.CSVReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class FileParser {

    private CSVReader sr;

    FileParser(String fileName) throws FileNotFoundException {
        this.sr = new CSVReader(new FileReader(fileName));

    }

    public String getLine() throws IOException {
        String[] line;
        if ((line = sr.readNext()) != null) {
            //splitLine = line[0].split(" ");
            return line[0];
        }
        return null;
    }
}
