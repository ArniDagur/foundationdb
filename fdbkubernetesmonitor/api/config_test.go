// config_test.go
//
// This source file is part of the FoundationDB open source project
//
// Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package api

import (
	"encoding/json"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func loadConfigFromFile(path string) *ProcessConfiguration {
	file, err := os.Open(path)
	Expect(err).NotTo(HaveOccurred())
	defer func() {
		Expect(file.Close()).NotTo(HaveOccurred())
	}()
	decoder := json.NewDecoder(file)
	config := &ProcessConfiguration{}
	Expect(decoder.Decode(config)).NotTo(HaveOccurred())

	return config
}

var _ = Describe("Testing FDB Kubernetes Monitor API", func() {
	When("generating arguments for default config", func() {
		var arguments []string

		BeforeEach(func() {
			var err error
			config := loadConfigFromFile(".testdata/default_config.json")
			Expect(config.Version).To(Equal(&Version{Major: 6, Minor: 3, Patch: 15}))
			arguments, err = config.GenerateArguments(1, map[string]string{
				"FDB_PUBLIC_IP":   "10.0.0.1",
				"FDB_POD_IP":      "192.168.0.1",
				"FDB_ZONE_ID":     "zone1",
				"FDB_INSTANCE_ID": "storage-1",
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should generate the expected arguments", func() {
			Expect(arguments).To(HaveExactElements([]string{
				"--cluster-file", ".testdata/fdb.cluster",
				"--public-address", "10.0.0.1:4501", "--listen-address", "192.168.0.1:4501",
				"--datadir", ".testdata/data/1", "--class", "storage",
				"--locality-zoneid", "zone1", "--locality-instance-id", "storage-1",
				"--locality-process-id", "storage-1-1",
			}))
		})
	})

	When("generating arguments for default config with a binary path specified", func() {
		var arguments []string

		BeforeEach(func() {
			var err error
			config := loadConfigFromFile(".testdata/default_config.json")
			Expect(err).NotTo(HaveOccurred())
			config.BinaryPath = "/usr/bin/fdbserver"

			arguments, err = config.GenerateArguments(1, map[string]string{
				"FDB_PUBLIC_IP":   "10.0.0.1",
				"FDB_POD_IP":      "192.168.0.1",
				"FDB_ZONE_ID":     "zone1",
				"FDB_INSTANCE_ID": "storage-1",
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should generate the expected arguments", func() {
			Expect(arguments).To(HaveExactElements([]string{
				"/usr/bin/fdbserver",
				"--cluster-file", ".testdata/fdb.cluster",
				"--public-address", "10.0.0.1:4501", "--listen-address", "192.168.0.1:4501",
				"--datadir", ".testdata/data/1", "--class", "storage",
				"--locality-zoneid", "zone1", "--locality-instance-id", "storage-1",
				"--locality-process-id", "storage-1-1",
			}))
		})
	})

	When("generating arguments for environment variable", func() {
		var argument string
		var err error
		var env map[string]string

		JustBeforeEach(func() {
			testArgument := Argument{ArgumentType: EnvironmentArgumentType, Source: "FDB_ZONE_ID"}
			argument, err = testArgument.GenerateArgument(1, env)
		})

		When("the env variable is present", func() {
			BeforeEach(func() {
				env = map[string]string{"FDB_ZONE_ID": "zone1", "FDB_MACHINE_ID": "machine1"}
			})

			It("should generate the expected arguments", func() {
				Expect(err).NotTo(HaveOccurred())
				Expect(argument).To(Equal("zone1"))
			})
		})

		When("the env variable is absent", func() {
			BeforeEach(func() {
				env = map[string]string{"FDB_MACHINE_ID": "machine1"}
			})

			It("should generate the expected arguments", func() {
				Expect(err).To(HaveOccurred())
			})
		})

	})

	When("generating arguments for IPList arguments", func() {
		var argument string
		var err error
		var env map[string]string
		var IPFamily int

		JustBeforeEach(func() {
			testArgument := Argument{ArgumentType: IPListArgumentType, Source: "FDB_PUBLIC_IP", IPFamily: IPFamily}
			argument, err = testArgument.GenerateArgument(1, env)
		})

		When("using IP Family 4", func() {
			BeforeEach(func() {
				IPFamily = 4
			})

			When("the env variable is present and has one address with the right IP family", func() {
				BeforeEach(func() {
					env = map[string]string{"FDB_PUBLIC_IP": "127.0.0.1,::1"}
				})

				It("should generate the expected arguments", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(argument).To(Equal("127.0.0.1"))
				})
			})

			When("the env variable is present and has one address with the right IP family with IPv6 first", func() {
				BeforeEach(func() {
					env = map[string]string{"FDB_PUBLIC_IP": "::1,127.0.0.1"}
				})

				It("should generate the expected arguments", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(argument).To(Equal("127.0.0.1"))
				})
			})

			When("no IPv4 address is present", func() {
				BeforeEach(func() {
					env = map[string]string{"FDB_PUBLIC_IP": "::1"}
				})

				It("should return an error", func() {
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("could not find IP with family 4"))
					Expect(argument).To(BeEmpty())
				})
			})
		})

		When("using IP Family 6", func() {
			BeforeEach(func() {
				IPFamily = 6
			})

			When("the env variable is present and has one address with the right IP family", func() {
				BeforeEach(func() {
					env = map[string]string{"FDB_PUBLIC_IP": "127.0.0.1,::1"}
				})

				It("should generate the expected arguments", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(argument).To(Equal("::1"))
				})
			})

			When("the env variable is present and has one address with the right IP family with IPv6 first", func() {
				BeforeEach(func() {

					env = map[string]string{"FDB_PUBLIC_IP": "::1,127.0.0.1"}
				})

				It("should generate the expected arguments", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(argument).To(Equal("::1"))
				})
			})

			When("a bad address is present", func() {
				BeforeEach(func() {
					env = map[string]string{"FDB_PUBLIC_IP": "bad,::1"}
				})

				It("should generate the expected arguments", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(argument).To(Equal("::1"))
				})
			})

			When("no IPv6 address is present", func() {
				BeforeEach(func() {
					env = map[string]string{"FDB_PUBLIC_IP": "127.0.0.1"}
				})

				It("return an error", func() {
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("could not find IP with family 6"))
					Expect(argument).To(BeEmpty())
				})
			})
		})

		When("using an invalid IP Family", func() {
			BeforeEach(func() {
				IPFamily = 5
			})

			It("should return an error", func() {
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("unsupported IP family 5"))
				Expect(argument).To(BeEmpty())
			})
		})
	})

	When("generating arguments for Sum arguments", func() {
		var argument string
		var err error
		var testArgument Argument
		var processNumber int
		var env map[string]string

		BeforeEach(func() {
			processNumber = 1
			env = map[string]string{"FDB_PUBLIC_IP": "10.0.0.1", "FDB_PORT_BLOCK_START": "4530"}
		})

		JustBeforeEach(func() {
			argument, err = testArgument.GenerateArgument(processNumber, env)
		})

		When("summing an environment variable and a process number", func() {
			BeforeEach(func() {
				testArgument = Argument{ArgumentType: SumArgumentType, Values: []Argument{
					{ArgumentType: EnvironmentArgumentType, Source: "FDB_PORT_BLOCK_START"},
					{ArgumentType: ProcessNumberArgumentType, Multiplier: 2, Offset: -2},
				}}
			})

			It("should generate the sum for the first process", func() {
				Expect(err).NotTo(HaveOccurred())
				Expect(argument).To(Equal("4530"))
			})

			When("generating the argument for the third process", func() {
				BeforeEach(func() {
					processNumber = 3
				})

				It("should generate the sum for the third process", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(argument).To(Equal("4534"))
				})
			})
		})

		When("the sum is part of a concatenated argument", func() {
			BeforeEach(func() {
				testArgument = Argument{ArgumentType: ConcatenateArgumentType, Values: []Argument{
					{Value: "--public_address="},
					{ArgumentType: EnvironmentArgumentType, Source: "FDB_PUBLIC_IP"},
					{Value: ":"},
					{ArgumentType: SumArgumentType, Values: []Argument{
						{ArgumentType: EnvironmentArgumentType, Source: "FDB_PORT_BLOCK_START"},
						{ArgumentType: ProcessNumberArgumentType, Multiplier: 2, Offset: -1},
					}},
				}}
				processNumber = 2
			})

			It("should generate the expected argument", func() {
				Expect(err).NotTo(HaveOccurred())
				Expect(argument).To(Equal("--public_address=10.0.0.1:4533"))
			})
		})

		When("summing literal values", func() {
			BeforeEach(func() {
				testArgument = Argument{ArgumentType: SumArgumentType, Values: []Argument{
					{Value: "10"},
					{ArgumentType: LiteralArgumentType, Value: "-3"},
				}}
			})

			It("should generate the sum", func() {
				Expect(err).NotTo(HaveOccurred())
				Expect(argument).To(Equal("7"))
			})
		})

		When("no values are provided", func() {
			BeforeEach(func() {
				testArgument = Argument{ArgumentType: SumArgumentType}
			})

			It("should generate zero", func() {
				Expect(err).NotTo(HaveOccurred())
				Expect(argument).To(Equal("0"))
			})
		})

		When("a value is not an integer", func() {
			BeforeEach(func() {
				testArgument = Argument{ArgumentType: SumArgumentType, Values: []Argument{
					{ArgumentType: EnvironmentArgumentType, Source: "FDB_PUBLIC_IP"},
					{Value: "1"},
				}}
			})

			It("should return an error", func() {
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(HavePrefix("value \"10.0.0.1\" of sum argument is not an integer"))
				Expect(argument).To(BeEmpty())
			})
		})

		When("an environment variable is missing", func() {
			BeforeEach(func() {
				testArgument = Argument{ArgumentType: SumArgumentType, Values: []Argument{
					{ArgumentType: EnvironmentArgumentType, Source: "FDB_MISSING"},
					{Value: "1"},
				}}
			})

			It("should return an error", func() {
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("missing environment variable FDB_MISSING"))
				Expect(argument).To(BeEmpty())
			})
		})

		When("the argument is parsed from JSON", func() {
			BeforeEach(func() {
				testArgument = Argument{}
				Expect(json.Unmarshal([]byte(`{"type": "Sum", "values": [
					{"type": "Environment", "source": "FDB_PORT_BLOCK_START"},
					{"type": "ProcessNumber", "multiplier": 2, "offset": -2}
				]}`), &testArgument)).To(Succeed())
				processNumber = 2
			})

			It("should generate the sum", func() {
				Expect(err).NotTo(HaveOccurred())
				Expect(argument).To(Equal("4532"))
			})
		})
	})

	When("marshalling a process configuration", func() {
		var out string

		BeforeEach(func() {
			config := &ProcessConfiguration{
				Version: &Version{
					Major: 7,
					Minor: 1,
					Patch: 57,
				},
			}

			data, err := json.Marshal(config)
			Expect(err).NotTo(HaveOccurred())
			out = string(data)
		})

		It("should parse it correct", func() {
			Expect(out).To(Equal("{\"version\":\"7.1.57\"}"))
		})
	})
})
