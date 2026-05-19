package server

import "testing"

// TestGCSClientOptions covers the credential fallback added for #347: a load
// from gs:// must not hard-fail with "could not find default credentials"
// when no GCS emulator and no Application Default Credentials are configured.
func TestGCSClientOptions(t *testing.T) {
	t.Run("emulator host configured", func(t *testing.T) {
		t.Setenv("STORAGE_EMULATOR_HOST", "http://localhost:1234")
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
		if got := len(gcsClientOptions(true)); got != 3 {
			t.Errorf("expected 3 options for emulator reads, got %d", got)
		}
		if got := len(gcsClientOptions(false)); got != 2 {
			t.Errorf("expected 2 options for emulator writes, got %d", got)
		}
	})
	t.Run("no credentials falls back to anonymous", func(t *testing.T) {
		t.Setenv("STORAGE_EMULATOR_HOST", "")
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
		if got := len(gcsClientOptions(true)); got != 1 {
			t.Errorf("expected anonymous fallback (1 option), got %d", got)
		}
	})
	t.Run("application default credentials honored", func(t *testing.T) {
		t.Setenv("STORAGE_EMULATOR_HOST", "")
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/path/to/creds.json")
		if got := gcsClientOptions(true); got != nil {
			t.Errorf("expected no extra options when ADC is set, got %d", len(got))
		}
	})
}
