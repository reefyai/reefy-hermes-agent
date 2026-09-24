# Reefy Hermes Agent

Reefy app-catalog wrapper for the upstream Nous Research Hermes Agent Docker image.

The Reefy manifest lives at `reefy/app.json`. Reefy service imports it with `reefy-deploy admin app-publish hermes-agent --ref <ref>`.

Current package: `v2026.9.24-reefy.1`, using upstream Hermes Agent v0.21.5
(`nousresearch/hermes-agent:v2026.9.24`). The data location, UID, dashboard
ports, and loopback relay behavior are unchanged. The relay seed filename is
versioned so existing installations receive it when upgrading.
