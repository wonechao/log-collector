package io.sugo.collect.reader.file;


import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.lang.StringUtils;

import javax.swing.filechooser.FileFilter;
import java.io.File;
import java.util.regex.Pattern;

/**
 * Created by fengxj on 4/10/17.
 */
public class SugoFileFilter implements IOFileFilter {

  private final Pattern pattern;
  private final String lastFileName;
  private final long lastFileOffset;

  public SugoFileFilter(String regEx, String lastFileName, long lastFileOffset) {
    this.pattern = Pattern.compile(regEx);
    this.lastFileName = lastFileName;
    this.lastFileOffset = lastFileOffset;

  }

  @Override
  public boolean accept(File file) {
    String fileName = file.getName();
    boolean match = pattern.matcher(fileName).matches();
    if (match) {
      if (StringUtils.isNotBlank(lastFileName)) {
        int comp = lastFileName.compareTo(fileName);
        if (comp > 0) {
          return false;
        } else if (comp == 0) {
          return file.length() > lastFileOffset;
        }
      }
      return true;
    }

    return false;
  }

  @Override
  public boolean accept(File dir, String name) {
    return false;
  }


}
