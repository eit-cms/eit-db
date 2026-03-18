package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	version = "0.4.0"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:     "eit-db-cli",
		Aliases: []string{"eit-migrate"},
		Short:   "EIT Database CLI",
		Long:    `A unified CLI for eit-db, including migration workflows and adapter scaffolding.`,
	}

	rootCmd.AddCommand(initCmd())
	rootCmd.AddCommand(generateCmd())
	rootCmd.AddCommand(adapterCmd())
	rootCmd.AddCommand(upCmd())
	rootCmd.AddCommand(downCmd())
	rootCmd.AddCommand(statusCmd())
	rootCmd.AddCommand(versionCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version number",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("eit-db-cli version %s (alias: eit-migrate)\n", version)
		},
	}
}
