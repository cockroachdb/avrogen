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
const BucketPathFlag = "bucket-path"
const SortedFlag = "sorted"
const LocalDirFlag = "local-dir"

// createCmd represents the create command
var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Create Avro files",
	Long: `Create Avro files and save locally and to GCS bucket.
`,
	Run: func(cmd *cobra.Command, args []string) {
		files := viper.GetInt(FilesFlag)
		size := viper.GetInt(SizeFlag)
		bucket := viper.GetString(BucketFlag)
		bucketPath := viper.GetString(BucketPathFlag)
		sorted := viper.GetBool(SortedFlag)
		localDir := viper.GetString(LocalDirFlag)
		tools.GenerateAvroFiles(files, size, bucket, bucketPath, sorted, localDir)
	},
}

func init() {
	rootCmd.AddCommand(createCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// createCmd.PersistentFlags().String("foo", "", "A help for foo")
	createCmd.PersistentFlags().Int(FilesFlag, 10, "Number of files to generate")
	createCmd.PersistentFlags().Int(SizeFlag, 10000, "Number of records to generate per file")
	createCmd.PersistentFlags().String(BucketFlag, "", "Cloud storage bucket name")
	createCmd.PersistentFlags().String(BucketPathFlag, string(time.Now().Format(time.RFC3339)), "Bucket path")
	createCmd.PersistentFlags().Bool(SortedFlag, false, "Generate data sorted by handle, within and across files")
	createCmd.PersistentFlags().String(LocalDirFlag, "", "Local directory to save files to")
	viper.BindPFlags(createCmd.PersistentFlags())

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// createCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
