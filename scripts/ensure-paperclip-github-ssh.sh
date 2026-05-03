#!/usr/bin/env bash
# Idempotent: add OpenSSH user config so git/ssh to github.com uses the Paperclip
# deploy key. OpenSSH resolves default identity paths from the passwd home
# (e.g. /home/node/.ssh), not from $HOME, so keys under /paperclip/.ssh are
# ignored unless we set IdentityFile explicitly. See CAN-23 / CAN-20 follow-up.
set -euo pipefail

readonly MARKER="# paperclip-managed-github-deploy-key (CAN-23)"
KEY_PATH="${PAPERCLIP_GITHUB_DEPLOY_KEY:-/paperclip/.ssh/id_ed25519}"
PASSWD_HOME="$(getent passwd "$(id -u)" | cut -d: -f6)"
SSH_DIR="${PAPERCLIP_SSH_USER_CONFIG_DIR:-$PASSWD_HOME/.ssh}"
CONFIG_PATH="$SSH_DIR/config"

if [[ ! -f "$KEY_PATH" ]]; then
  echo "ensure-paperclip-github-ssh: no key at $KEY_PATH; nothing to do." >&2
  exit 0
fi

mkdir -p "$SSH_DIR"
chmod 700 "$SSH_DIR"

if [[ -f "$CONFIG_PATH" ]] && grep -qF "$MARKER" "$CONFIG_PATH" 2>/dev/null; then
  echo "ensure-paperclip-github-ssh: marker already present in $CONFIG_PATH" >&2
  exit 0
fi

{
  echo ""
  echo "$MARKER"
  echo "Host github.com"
  echo "  IdentityFile $KEY_PATH"
  echo "  IdentitiesOnly yes"
} >>"$CONFIG_PATH"

chmod 600 "$CONFIG_PATH"
echo "ensure-paperclip-github-ssh: appended github.com stanza to $CONFIG_PATH" >&2
