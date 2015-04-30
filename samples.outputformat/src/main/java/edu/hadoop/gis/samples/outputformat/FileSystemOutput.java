package edu.hadoop.gis.samples.outputformat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * An abstraction for v1 and v2 API accessing the filesystem and obtaining the output path.
 * 
 * @author Jan Tschada
 *
 */
public class FileSystemOutput {

	private final FileSystem fileSystem;
	private final Path outputPath;
	
	public FileSystemOutput(FileSystem fileSystem, Path outputPath) {
		this.fileSystem = fileSystem;
		this.outputPath = outputPath;
	}

	public FileSystem getFileSystem() {
		return fileSystem;
	}

	public Path getOutputPath() {
		return outputPath;
	}
}
