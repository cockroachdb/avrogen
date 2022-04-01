/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"github.com/cockroachlabs/avrogen/tools"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"time"
)

const FilesFlag = "files"
const SizeFlag = "size"
const BucketFlag = "bucket"
const BucketPrefixFlag = "bucket-prefix"
const SortedFlag = "sorted"
const LocalDirFlag = "local-dir"

// createCmd represents the create command
var createCmd = &cobra.Command{
	Use:   "create",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		files := viper.GetInt(FilesFlag)
		size := viper.GetInt(SizeFlag)
		bucket := viper.GetString(BucketFlag)
		bucketPrefix := viper.GetString(BucketPrefixFlag)
		sorted := viper.GetBool(SortedFlag)
		localDir := viper.GetString(LocalDirFlag)
		tools.GenerateAvroFiles(files, size, bucket, bucketPrefix, sorted, localDir)
	},
}

func init() {
	rootCmd.AddCommand(createCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// createCmd.PersistentFlags().String("foo", "", "A help for foo")
	createCmd.PersistentFlags().Int(FilesFlag, 10, "Number of files to generate")
	createCmd.PersistentFlags().Int(SizeFlag, 100, "File size in MiB")
	createCmd.PersistentFlags().String(BucketFlag, "jon-twitter", "Cloud storage bucket name")
	createCmd.PersistentFlags().String(BucketPrefixFlag, string(time.Now().Format(time.RFC3339)), "Bucket prefix")
	createCmd.PersistentFlags().Bool(SortedFlag, false, "Generate data sorted by handle, across within and across files")
	createCmd.PersistentFlags().String(LocalDirFlag, "avro-data", "Local directory")
	viper.BindPFlags(createCmd.PersistentFlags())

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// createCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
