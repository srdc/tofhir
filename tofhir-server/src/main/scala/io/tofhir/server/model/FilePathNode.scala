package io.tofhir.server.model

/**
 * Represents a node for each file in the file system
 * @param label The name of the file
 * @param isFolder True if the file is a folder
 * @param path file path of the node
 * @param children The children of the folder
 */
case class FilePathNode(label: String, isFolder: Boolean, path: String, children: List[FilePathNode])
