package main

import (
	"testing"
	"time"

	"github.com/truenas/truenas-csi/pkg/driver"
)

// The deployment manifests configure metrics through the environment rather than
// the flag, because an older driver image ignores an unknown environment variable
// but exits on an unknown flag. Both paths have to work, with the flag winning when
// an operator passes it explicitly.
func TestLoadEnvConfig_MetricsAddr(t *testing.T) {
	tests := []struct {
		name string
		flag string
		env  string
		want string
	}{
		{"disabled by default", "", "", ""},
		{"from the environment", "", ":8080", ":8080"},
		{"from the flag", ":9090", "", ":9090"},
		{"flag wins over the environment", ":9090", ":8080", ":9090"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TRUENAS_URL", "wss://truenas.example")
			t.Setenv("TRUENAS_API_KEY", "key")
			t.Setenv("TRUENAS_DEFAULT_POOL", "tank")
			t.Setenv("TRUENAS_METRICS_ADDR", tt.env)

			config := &driver.DriverConfig{MetricsAddr: tt.flag}
			if err := loadEnvConfig(config); err != nil {
				t.Fatalf("loadEnvConfig() = %v, want nil", err)
			}
			if config.MetricsAddr != tt.want {
				t.Errorf("MetricsAddr = %q, want %q", config.MetricsAddr, tt.want)
			}
		})
	}
}

func TestLoadEnvConfig_CABundle(t *testing.T) {
	t.Setenv("TRUENAS_URL", "wss://truenas.example")
	t.Setenv("TRUENAS_API_KEY", "key")
	t.Setenv("TRUENAS_DEFAULT_POOL", "tank")
	t.Setenv(envCABundle, "/etc/truenas-csi/trusted-ca/ca-bundle.crt")

	config := &driver.DriverConfig{}
	if err := loadEnvConfig(config); err != nil {
		t.Fatalf("loadEnvConfig() = %v, want nil", err)
	}
	if config.TrueNASCABundle != "/etc/truenas-csi/trusted-ca/ca-bundle.crt" {
		t.Errorf("TrueNASCABundle = %q, want the path from %s", config.TrueNASCABundle, envCABundle)
	}
}

func TestLoadEnvConfig_MountHeal(t *testing.T) {
	t.Setenv("TRUENAS_URL", "wss://truenas.example")
	t.Setenv("TRUENAS_API_KEY", "key")
	t.Setenv("TRUENAS_DEFAULT_POOL", "tank")

	t.Run("disabled unless an interval is set", func(t *testing.T) {
		t.Setenv("TRUENAS_MOUNT_HEAL_INTERVAL", "")
		config := &driver.DriverConfig{}
		if err := loadEnvConfig(config); err != nil {
			t.Fatal(err)
		}
		if config.MountHeal.Interval != 0 {
			t.Errorf("Interval = %v, want 0", config.MountHeal.Interval)
		}
	})

	t.Run("read from the environment", func(t *testing.T) {
		t.Setenv("TRUENAS_MOUNT_HEAL_INTERVAL", "45s")
		t.Setenv("TRUENAS_MOUNT_HEAL_COOLDOWN", "15m")
		t.Setenv("TRUENAS_KUBELET_DIR", "/var/snap/microk8s/common/var/lib/kubelet")
		config := &driver.DriverConfig{}
		if err := loadEnvConfig(config); err != nil {
			t.Fatal(err)
		}
		if got := config.MountHeal; got.Interval != 45*time.Second || got.Cooldown != 15*time.Minute ||
			got.KubeletDir != "/var/snap/microk8s/common/var/lib/kubelet" {
			t.Errorf("MountHeal = %+v", got)
		}
	})

	for _, name := range []string{"TRUENAS_MOUNT_HEAL_INTERVAL", "TRUENAS_MOUNT_HEAL_COOLDOWN"} {
		t.Run("invalid "+name, func(t *testing.T) {
			t.Setenv(name, "soon")
			if err := loadEnvConfig(&driver.DriverConfig{}); err == nil {
				t.Errorf("loadEnvConfig() with an invalid %s should fail", name)
			}
		})
	}
}

func TestLoadEnvConfig_ISCSIReplacementTimeout(t *testing.T) {
	t.Setenv("TRUENAS_URL", "wss://truenas.example")
	t.Setenv("TRUENAS_API_KEY", "key")
	t.Setenv("TRUENAS_DEFAULT_POOL", "tank")

	for _, tt := range []struct {
		env     string
		want    time.Duration
		wantErr bool
	}{
		{env: "", want: 0},
		{env: "5m", want: 5 * time.Minute},
		{env: "300ms", wantErr: true},
		{env: "five", wantErr: true},
	} {
		t.Run(tt.env, func(t *testing.T) {
			t.Setenv("TRUENAS_ISCSI_REPLACEMENT_TIMEOUT", tt.env)
			config := &driver.DriverConfig{}
			err := loadEnvConfig(config)
			if (err != nil) != tt.wantErr {
				t.Fatalf("loadEnvConfig() = %v, want error: %v", err, tt.wantErr)
			}
			if err == nil && config.ISCSIReplacementTimeout != tt.want {
				t.Errorf("ISCSIReplacementTimeout = %v, want %v", config.ISCSIReplacementTimeout, tt.want)
			}
		})
	}
}
