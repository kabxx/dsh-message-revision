window.__ModuleLoader__.load({
  id: '@kabxx/dsh-message-revision',
  factory(require) {
    const { Fragment, createElement: h, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState, useSyncExternalStore } = require('react');
    const locale = 'messageRevision';
    const strings = {
      zh: {
        edit: '编辑消息', cancel: '取消',
        busy: '回复生成中，请先停止或等待完成。', done: '消息已更新并重发',
        cleanupPending: '消息已更新并重发，部分旧记录待清理。',
        unchanged: '内容未修改', tooLong: '消息不能超过 200,000 个字符。',
        uploadWait: '附件仍在上传或上传失败，请处理后再发送。',
        fileUnavailable: '当前会话无法添加附件。', fileDirectory: '不支持直接添加文件夹，请选择单个文件。',
        anotherEdit: '请先完成或取消当前编辑。',
        missing: '未找到该消息，请刷新后重试。', unsupported: '此消息不支持编辑。',
        locked: '会话正被占用，暂时无法编辑。', changed: '会话状态已变更，请重新打开编辑框。',
        'composer-unavailable': '输入框未能加载，请刷新页面后重试。',
        'toolbar-update': '插件前后端版本不一致，请重新加载插件。',
        'child-conflict': '关联的子智能体会话无法回退，暂不支持编辑此消息。',
        syncing: '消息已更新，请重新打开会话刷新视图。',
        draftSaved: '草稿已保留。',
        draftConflict: '原消息已变更，请先复制草稿后关闭重试。',
        textPart: '正文', sendPart: '发送',
        'reconnect-unavailable': '会话连接不可用，请重新打开会话。',
        'authentication-required': '请先完成登录认证。', failed: '编辑失败，请重试。',
      },
      en: {
        edit: 'Edit message', cancel: 'Cancel',
        busy: 'Reply in progress. Stop or wait for it to finish.', done: 'Message updated and resent',
        cleanupPending: 'Message updated and resent; some old records remain.',
        unchanged: 'No changes to send', tooLong: 'Message exceeds 200,000 characters.',
        uploadWait: 'Wait for uploads to finish or remove failed attachments.',
        fileUnavailable: 'Attachments are unavailable for this session.', fileDirectory: 'Folders are not supported; select individual files.',
        anotherEdit: 'Finish or cancel the current edit first.',
        missing: 'Message not found. Refresh and try again.', unsupported: 'This message cannot be edited.',
        locked: 'Session is in use and cannot be edited right now.', changed: 'Session state changed. Reopen the editor.',
        'composer-unavailable': 'Failed to load the editor. Refresh the page and try again.',
        'toolbar-update': 'Plugin version mismatch. Reload the plugin.',
        'child-conflict': 'Cannot rewind associated subagent sessions for this message.',
        syncing: 'Message updated. Reopen the session to refresh.',
        draftSaved: 'Draft preserved.',
        draftConflict: 'Original message changed. Copy your draft and reopen to edit.',
        textPart: 'Text block', sendPart: 'Send',
        'reconnect-unavailable': 'Session connection unavailable. Reopen this session.',
        'authentication-required': 'Sign in required.', failed: 'Failed to update message. Try again.',
      },
    };
    const messageSeat = '[data-slot="conversation.chat.user.before"]';
    const editingRow = `div:has(> ${messageSeat} > .dsh-revision-editor)`;
    const originalContent = `${editingRow} > ${messageSeat} + *`;
    const originalBubble = `${originalContent} > [data-message-bubble]`;
    const css = `
      /* rc.5 seats this contribution before the user bubble and Copy after it. */
      div:has(> ${messageSeat} > .dsh-revision-pencil):has(> [data-clock="start"]) { position: relative; }
      div:has(> ${messageSeat} > .dsh-revision-pencil) > [data-clock="start"] > button {
        margin-left: calc(36px + var(--dsh-content-font-delta, 0px));
      }
      div:has(> [data-clock="start"]) > ${messageSeat} > .dsh-revision-pencil { display: inline-grid; }
      .dsh-revision-pencil {
        position: absolute;
        right: calc(36px + var(--dsh-content-font-delta, 0px));
        bottom: 0;
        display: none;
        place-items: center;
        width: calc(28px + var(--dsh-content-font-delta, 0px));
        height: calc(28px + var(--dsh-content-font-delta, 0px));
        padding: 0;
        border: 0;
        border-radius: var(--dsw-radius-sm, 6px);
        background: transparent;
        color: var(--dsw-alias-label-tertiary);
        cursor: pointer;
      }
      .dsh-revision-pencil svg {
        width: calc(15px + var(--dsh-content-font-delta, 0px));
        height: calc(15px + var(--dsh-content-font-delta, 0px));
      }
      .dsh-revision-pencil:hover {
        background: var(--dsw-alias-interactive-bg-hover);
        color: var(--dsw-alias-label-secondary);
      }
      .dsh-revision-pencil[aria-busy="true"] { cursor: progress; }
      .dsh-revision-pencil::after {
        content: attr(data-tooltip);
        position: absolute;
        z-index: 2;
        top: calc(100% + 6px);
        left: 50%;
        transform: translateX(-50%);
        padding: 4px 8px;
        border-radius: var(--dsw-radius-sm, 6px);
        background: var(--dsw-alias-tooltip-bg);
        color: var(--dsw-alias-tooltip-label);
        font: 12px / 18px var(--dsw-font-family);
        white-space: nowrap;
        pointer-events: none;
        visibility: hidden;
        opacity: 0;
        transition: opacity 100ms ease;
      }
      .dsh-revision-pencil:not([data-tooltip-dismissed]):focus-visible::after {
        visibility: visible;
        opacity: 1;
      }
      @media (hover: hover) {
        .dsh-revision-pencil:not([data-tooltip-dismissed]):hover::after {
          visibility: visible;
          opacity: 1;
          transition-delay: 250ms;
        }
        [data-chat-flow-kind="user"]:has(~ :is([data-chat-flow-kind="user"], [data-chat-flow-kind="steering"])) .dsh-revision-pencil {
          opacity: 0;
        }
        [data-chat-flow-kind="user"]:has(~ :is([data-chat-flow-kind="user"], [data-chat-flow-kind="steering"])):is(:hover, :focus-within) .dsh-revision-pencil {
          opacity: 1;
        }
      }
      /* The native composer renders the editable attachment rail. Keep the original
         message's references and extra blocks visible around it. */
      ${editingRow} > [data-clock="start"],
      ${originalContent}:has(> [data-message-bubble]:only-child > span:only-child),
      ${originalBubble}:has(> span:only-child),
      ${originalBubble} > span:first-child,
      ${originalContent} > [data-message-attachments] { display: none; }
      .dsh-revision-editor {
        box-sizing: border-box;
        order: 1;
        align-self: center;
        width: calc(100% + 32px);
        min-width: 0;
        --dsh-composer-side-clearance: 0px;
      }
      .dsh-revision-text-part { display: grid; gap: 4px; margin: 6px 0; }
      .dsh-revision-text-part textarea {
        box-sizing: border-box;
        padding: 6px 10px;
        color: inherit;
        font: inherit;
        background: transparent;
        border: 1px solid var(--dsw-alias-border-primary, currentColor);
        border-radius: var(--dsw-radius-sm, 6px);
        field-sizing: content;
        resize: none;
      }
      .dsh-revision-editor [data-composer-card] > :has(> .dsh-revision-cancel) { display: contents; }
      .dsh-revision-editor .dsh-revision-cancel {
        position: absolute;
        z-index: 2;
        top: 6px;
        right: 8px;
        display: grid;
        place-items: center;
        width: 28px;
        height: 28px;
        padding: 0;
        border: 0;
        border-radius: var(--dsw-radius-sm);
        background: transparent;
        color: var(--dsw-alias-label-secondary);
        cursor: pointer;
      }
      .dsh-revision-editor .dsh-revision-cancel:not(:disabled):hover {
        background: var(--dsw-alias-interactive-bg-hover);
      }
      /* Preserve the native scrollport and reserve space for the close button. */
      .dsh-revision-editor [contenteditable] { padding-inline-end: 36px; }
      .dsh-revision-feedback {
        position: fixed;
        z-index: 1000;
        top: var(--dsw-spacing-xl, 24px);
        left: 50%;
        max-width: min(90vw, 32rem);
        padding: 8px 12px;
        transform: translateX(-50%);
        border-radius: var(--dsw-radius-sm);
        background: var(--dsw-alias-tooltip-bg);
        color: var(--dsw-alias-tooltip-label);
      }
    `;

    /*!
     * Tabler Icons v3.31.0, outline pencil and x.
     * https://github.com/tabler/tabler-icons/tree/v3.31.0/icons/outline
     * MIT License
     * Copyright (c) 2020-2024 Paweł Kuna
     *
     * Permission is hereby granted, free of charge, to any person obtaining a copy
     * of this software and associated documentation files (the "Software"), to deal
     * in the Software without restriction, including without limitation the rights
     * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
     * copies of the Software, and to permit persons to whom the Software is
     * furnished to do so, subject to the following conditions:
     *
     * The above copyright notice and this permission notice shall be included in all
     * copies or substantial portions of the Software.
     *
     * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
     * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
     * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
     * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
     * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
     * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
     * SOFTWARE.
     */
    const tablerPaths = {
      pencil: ['M4 20h4l10.5 -10.5a2.828 2.828 0 1 0 -4 -4l-10.5 10.5v4', 'M13.5 6.5l4 4'],
      x: ['M18 6l-12 12', 'M6 6l12 12'],
    };

    function TablerIcon({ name, size = 16 }) {
      return h('svg', { width: size, height: size, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor',
        strokeWidth: 2, strokeLinecap: 'round', strokeLinejoin: 'round', focusable: 'false', 'aria-hidden': true },
      tablerPaths[name].map(d => h('path', { d, key: d })));
    }

    const composerSlot = 'conversation.composer.bar';
    const editorFactory = 'dsh-message-revision.editor';
    const toastFactory = 'workspace.toast.content';
    const emptyUploads = Object.freeze({});
    const emptyUploadSource = Object.freeze({ getSnapshot: () => emptyUploads, subscribe: () => () => {} });
    const closedMenu = select => select(null);

    function replaceMethod(object, key, next) {
      const previous = Object.getOwnPropertyDescriptor(object, key);
      Object.defineProperty(object, key, { configurable: true, writable: true, value: next });
      return () => {
        if (object[key] !== next) return;
        if (previous) Object.defineProperty(object, key, previous);
        else delete object[key];
      };
    }

    function sessionRuntime(ctx) {
      const service = ctx.get('sessions');
      return service?.[Symbol.for('cordis.original')] ?? service;
    }

    function sessionView(sessions, sessionId) {
      const binding = sessions.binding?.(sessionId);
      const session = binding?.session;
      if (typeof session?.resync !== 'function' || typeof session.getSnapshot !== 'function'
        || typeof session.projections?.clear !== 'function') return undefined;
      return binding;
    }

    function holdComposerSubmission(binding) {
      const service = binding.ctx?.get('conversation');
      const conversation = service?.[Symbol.for('cordis.original')] ?? service;
      const send = conversation?.sendSession;
      if (typeof send !== 'function') return () => {};
      return replaceMethod(conversation, 'sendSession', function (session, ...args) {
        if (session?.sessionId === binding.sessionId) return Promise.resolve({ kind: 'error' });
        return send.call(this, session, ...args);
      });
    }

    function rebindComposerAttachments(binding) {
      const conversation = binding.ctx?.get('conversation');
      if (typeof conversation?.rebindDraftFiles !== 'function') return;
      const input = conversation.input.for(binding.ctx);
      const { attachmentIds } = input.state.getSnapshot();
      if (attachmentIds.length > 0) {
        // Reuse the File objects and draft IDs. The native service marks them
        // uploading immediately and replaces receipts scoped to the old Agent.
        conversation.rebindDraftFiles(binding.sessionId, attachmentIds, binding.session.getSnapshot().subagent === null);
      }
    }

    async function resyncSessionView(sessions, sessionId, binding) {
      const { manager } = sessions;
      manager.projectionInflight?.get(sessionId)?.controller.abort();
      manager.projectionInflight?.delete(sessionId);
      manager.projectionLoads?.delete(sessionId);
      binding.session.projections.clear();
      await binding.session.resync();
    }

    function watchSessionViews(sessions) {
      const manager = sessions?.manager;
      const remove = manager?.handleSessionRemoved;
      const add = manager?.handleSessionAdded;
      if (typeof remove !== 'function' || typeof add !== 'function') return undefined;
      const holds = new Map();
      const transitions = new WeakMap();
      let disposed = false;

      const undoRemove = replaceMethod(manager, 'handleSessionRemoved', function (id) {
        const binding = sessionView(sessions, id);
        const transition = binding && transitions.get(binding);
        if (transition) transition.removal++;
        const hold = holds.get(id);
        if (hold) {
          hold.removed();
          return;
        }
        return remove.call(this, id);
      });
      let undoAdd;
      try {
        undoAdd = replaceMethod(manager, 'handleSessionAdded', function (summary) {
          const id = summary.sessionId;
          const binding = sessionView(sessions, id);
          const session = binding?.session;
          // Native removal can discard the Manager's entry while the retained
          // Session still uses the store. Rejoin before the new list seed lands.
          if (summary.agentAvailable === true && session?.getSnapshot().removed
            && manager.projectionStores?.get(id) !== session.projections) {
            manager.projectionStores?.set(id, session.projections);
          }
          const result = add.call(this, summary);
          if (disposed || summary.agentAvailable !== true) return result;
          const hold = holds.get(id);
          if (hold) {
            hold.added();
            return result;
          }
          if (!session?.getSnapshot().removed || session.removed !== true || typeof session.notifier?.markDirty !== 'function') {
            return result;
          }
          // Each tab receives the native event pair. An ordinary deletion remains
          // removed. A live addition for the same ID establishes a new history
          // generation, including when another tab performed the rewrite.
          let transition = transitions.get(binding);
          if (!transition) {
            transition = { removal: 0, scheduled: -1, work: Promise.resolve() };
            transitions.set(binding, transition);
          }
          const removal = transition.removal;
          if (transition.scheduled === removal) return result;
          transition.scheduled = removal;
          const current = () => !disposed && sessions.binding(id) === binding && transition.removal === removal;
          transition.work = transition.work.then(async () => {
            if (!current()) return;
            rebindComposerAttachments(binding);
            await resyncSessionView(sessions, id, binding);
            if (!current() || session.getSnapshot().openState !== 'open') return;
            // rc.5's native removal flag is one-way on a retained Session.
            session.removed = false;
            session.notifier.markDirty();
          }).catch(() => {
            // A failed sync leaves the view unavailable. Allow a later addition to retry.
          }).finally(() => {
            if (transition.scheduled === removal) transition.scheduled = -1;
          });
          return result;
        });
      } catch (error) {
        undoRemove();
        throw error;
      }

      function holdSessionView(sessionId) {
        const binding = sessionView(sessions, sessionId);
        const session = binding?.session;
        if (disposed || !binding || session.getSnapshot().removed || holds.has(sessionId)) {
          throw new Error('reconnect-unavailable');
        }
        let removed = false;
        let readded = false;
        let rebound = false;
        let released = false;
        let notifyAddition;
        const releaseComposer = holdComposerSubmission(binding);
        const rebind = () => {
          if (rebound || disposed || released || sessions.binding(sessionId) !== binding) return;
          rebindComposerAttachments(binding);
          rebound = true;
          releaseComposer();
        };
        const hold = {
          removed() { removed = true; readded = false; rebound = false; },
          added() {
            if (!removed || readded) return;
            readded = true;
            notifyAddition?.();
            rebind();
          },
          release() {
            if (released) return;
            released = true;
            holds.delete(sessionId);
            notifyAddition?.();
            releaseComposer();
            if (removed && !readded) manager.handleSessionRemoved(sessionId);
          },
        };
        holds.set(sessionId, hold);
        return {
          get removed() { return removed; },
          release: hold.release,
          async reconnect() {
            if (!readded && !released) {
              // The HTTP response and the native event stream can arrive in either order.
              await new Promise(resolve => {
                const timer = setTimeout(() => notifyAddition?.(), 1200);
                notifyAddition = () => {
                  clearTimeout(timer);
                  notifyAddition = undefined;
                  resolve();
                };
              });
            }
            if (released || sessions.binding(sessionId) !== binding) return;
            await resyncSessionView(sessions, sessionId, binding);
            if (released || sessions.binding(sessionId) !== binding) return;
            const snapshot = session.getSnapshot();
            if (snapshot.openState !== 'open' || snapshot.removed) throw new Error('syncing');
            rebind();
          },
        };
      }

      return {
        holdSessionView,
        dispose() {
          disposed = true;
          for (const hold of holds.values()) hold.release();
          undoAdd();
          undoRemove();
        },
      };
    }

    function hookFor(source) {
      const subscribe = listener => source.subscribe(listener);
      const getSnapshot = () => source.getSnapshot();
      return select => select(useSyncExternalStore(subscribe, getSnapshot));
    }

    // rc.5 exposes the native bar as a slot entry, not a separately exported React component.
    // Take the component from its registration, without replacing the resident slot.
    function nativeComposerEntry(slots) {
      const entries = slots.entries?.(composerSlot) ?? [];
      const native = entries.filter(entry => entry.locale === 'conversation'
        && typeof entry.inject === 'function' && entry.children?.['conversation.input.model']
        && entry.children?.['conversation.input.attachments']);
      return native.length === 1 ? native[0] : undefined;
    }

    function revisionDraftKey(sessionId, messageId) {
      return JSON.stringify([sessionId, messageId]);
    }

    function snapshotRevisionDraft(edit, text, attachmentIds, textParts) {
      const { retained, conversation } = edit.composer;
      const retainedIndices = [];
      const fileIds = [];
      for (const id of attachmentIds) {
        if (retained.has(id)) retainedIndices.push(retained.get(id).index);
        else fileIds.push(id);
      }
      const available = new Map((conversation?.resolveDraftAttachments(fileIds) ?? []).map(item => [item.id, item.file]));
      const files = fileIds.map(id => available.get(id));
      if (files.some(file => !file)) throw new Error('fileUnavailable');
      return {
        text, textParts, files, retainedIndices, settings: { ...edit.settings },
        original: edit.original, originalTextParts: edit.originalTextParts, originalAttachments: edit.originalAttachments,
      };
    }

    function canRestoreRevisionDraft(saved, preview) {
      return saved.original === preview.text
        && JSON.stringify(saved.originalTextParts) === JSON.stringify(preview.textParts)
        && saved.originalAttachments === JSON.stringify(preview.attachments);
    }

    function seedRevisionDraft(shell, retained, uploaded, conversation, sessionId, text, saved) {
      shell.setDraft(saved?.text ?? text);
      const retainedIndices = saved && new Set(saved.retainedIndices);
      const ids = [...retained].filter(([, attachment]) => !retainedIndices || retainedIndices.has(attachment.index))
        .map(([id]) => id);
      for (const file of saved?.files ?? []) {
        const [attachment] = conversation?.createDrafts(sessionId, [file], true) ?? [];
        if (!attachment) throw new Error('fileUnavailable');
        uploaded.add(attachment.id);
        ids.push(attachment.id);
      }
      if (!shell.addAttachments(ids)) throw new Error('composer-unavailable');
    }

    function openDraft(ctx, native, sessionId, text, attachments, submit, saved) {
      const injected = native.inject(sessionId);
      const original = injected?.keyboard;
      if (typeof original?.constructor !== 'function' || typeof original.deps?.defaultSink !== 'function'
        || typeof original.editor?.getRootElement !== 'function' || !injected.hooks) {
        throw new Error('composer-unavailable');
      }
      const conversation = ctx.get('conversation');
      const imageService = ctx.get('uiConversation');
      const retained = new Map(attachments.map(({ index, block }) => {
        const ref = block.attachment;
        const image = block.type === 'image';
        return [`revision-attachment:${index}`, {
          index, image, ref,
          descriptor: {
            kind: image ? 'image' : 'file', id: `revision-attachment:${index}`,
            // Display metadata only. The original bytes and immutable native ref stay on the host.
            file: { name: ref.name ?? '', size: ref.bytes, type: ref.mediaType ?? '' },
          },
        }];
      }));
      const uploaded = new Set();
      const shell = new original.constructor({
        actx: original.deps.actx,
        quoteRemoveLabel: original.deps.quoteRemoveLabel,
        defaultSink: submit,
      });
      const useDraft = hookFor(shell.state);
      const useFileUploads = hookFor(conversation?.fileUploads ?? emptyUploadSource);
      function dispose() {
        shell.dispose();
        for (const id of uploaded) conversation.releaseDraftAttachment(id);
        uploaded.clear();
      }
      try {
        seedRevisionDraft(shell, retained, uploaded, conversation, sessionId, text, saved);
        return {
          shell, retained, conversation, imageService, uploaded, dispose, useDraft, useFileUploads,
          registerFileIntake: injected.registerFileIntake,
          useNotices: hookFor(shell.notices), useLexicon: hookFor(shell.lexicon),
          useBusyEnter: hookFor(injected.hooks.busyEnter), useStopShortcut: hookFor(injected.hooks.stopShortcut),
        };
      } catch (error) {
        dispose();
        throw error;
      }
    }

    function modelDirectoryWithSelection(directory, selection) {
      if (!selection || !directory) return directory;
      let previous, overlay;
      return {
        subscribe: listener => directory.subscribe(listener),
        getSnapshot() {
          const snapshot = directory.getSnapshot();
          if (previous !== snapshot) {
            previous = snapshot;
            overlay = { ...snapshot, current: selection };
          }
          return overlay;
        },
      };
    }

    function NativeEditor({ edit, dismiss, cancelLabel, renderSlot, t, ...props }) {
      const { shell, useDraft, useNotices, useLexicon, useBusyEnter, useStopShortcut,
        useFileUploads, retained, conversation, imageService, uploaded, registerFileIntake } = edit.composer;
      const phase = useDraft(state => state.phase);
      const pending = phase === 'sending' || phase === 'adjudicating' || phase === 'submitting';
      const [model, setModel] = useState(edit.settings.model);
      const [permission, setPermission] = useState(edit.settings.permissionPreset);
      const [previews, setPreviews] = useState({});
      const [, setPartsVersion] = useState(0);
      const draftText = useDraft(state => state.draft);
      const draftAttachments = useDraft(state => state.attachmentIds);
      const sendRemainingText = edit.additionalText.length > 0 && !draftText.trim()
        && draftAttachments.length === 0 && edit.additionalText.some(value => value.trim());
      const dropTarget = useRef(null);
      const modelDirectory = useMemo(() => modelDirectoryWithSelection(edit.modelDirectory, model),
        [edit, model]);
      const historicalUploads = useMemo(() => Object.fromEntries([...retained.keys()].map(id => [id, { status: 'ready' }])), [retained]);
      const useCombinedUploads = select => useFileUploads(snapshot => select({ ...historicalUploads, ...snapshot }));
      useEffect(() => {
        if (edit.restored) shell.focus();
      }, [edit, shell]);
      useEffect(() => {
        if (!imageService) return undefined;
        let mounted = true;
        for (const [id, item] of retained) {
          if (!item.image) continue;
          void imageService.imageUrl(props.sessionId, item.ref).then(url => {
            if (mounted) setPreviews(previous => ({ ...previous, [id]: url }));
          }).catch(() => { /* A missing preview does not discard the original ref. */ });
        }
        return () => { mounted = false; };
      }, [imageService, retained, props.sessionId]);

      const resolveDraftAttachments = useCallback(ids => {
        const picked = new Map((conversation?.resolveDraftAttachments(ids) ?? []).map(item => [item.id, item]));
        return ids.flatMap(id => {
          const existing = retained.get(id);
          if (existing) return [{ ...existing.descriptor, ...(existing.image ? { previewUrl: previews[id] } : {}) }];
          const current = picked.get(id);
          return current ? [current] : [];
        });
      }, [conversation, retained, previews]);
      function addFiles(files, directories) {
        if (directories?.size > 0) return edit.labels.fileDirectory;
        if (props.disabled || pending || typeof conversation?.createDrafts !== 'function') {
          return edit.labels.fileUnavailable;
        }
        const drafts = [];
        try {
          for (const file of files) drafts.push(...conversation.createDrafts(props.sessionId, [file], true));
          if (shell.addAttachments(drafts.map(item => item.id))) {
            for (const item of drafts) uploaded.add(item.id);
            return null;
          }
        } catch (error) {
          for (const item of drafts) conversation.releaseDraftAttachment(item.id);
          return error instanceof Error ? error.message : String(error);
        }
        for (const item of drafts) conversation.releaseDraftAttachment(item.id);
        return edit.labels.fileUnavailable;
      }
      function removeAttachment(id) {
        if (props.disabled || pending || !shell.removeAttachment(id)) return;
        if (uploaded.delete(id)) conversation.releaseDraftAttachment(id);
      }
      const usePermissionProjection = (key, select) => props.useProjection(key, value => {
        const view = key === 'permissions' && value !== undefined && value !== null && permission
          ? { ...value, currentValue: permission } : value;
        return select ? select(view) : view;
      });
      const renderEditorSlot = (name, owner) => {
        if (name === 'conversation.input.attachments') {
          dropTarget.current = owner;
          return renderSlot(name, { ...owner, canAcceptDrop: false });
        }
        if (name === 'conversation.input.permission') return renderSlot(name, {
          ...owner, useProjection: usePermissionProjection,
          select: async preset => {
            edit.settings.permissionPreset = preset;
            setPermission(preset);
            return true;
          },
        });
        if (name === 'conversation.input.model') return renderSlot(name, {
          ...owner,
          ...(modelDirectory === undefined ? { locked: true } : {
            directory: modelDirectory,
            select: async selection => {
              edit.settings.model = selection;
              setModel(selection);
              return { ok: true, value: undefined };
            },
          }),
        });
        if (name === 'conversation.input.activity') {
          return renderSlot(name, { ...owner, inputActions: shell.actions });
        }
        if (name === 'conversation.input.plan') return renderSlot(name, { ...owner, locked: true });
        if (name === 'conversation.composer.dock') return renderSlot(name, owner);
        // Extension controls address the resident draft through their session scope.
        if (name === 'conversation.input.left' || name === 'conversation.input.right') {
          return h('div', { inert: true }, renderSlot(name, owner));
        }
        return null;
      };
      function targetFileDrag(event, drop = false) {
        const transfer = event.dataTransfer;
        if (!transfer || !transfer.types.some(type => type === 'Files' || type === 'application/x-dsh-file')) return;
        event.preventDefault();
        event.stopPropagation();
        const target = dropTarget.current;
        transfer.dropEffect = target?.canAcceptDrop ? 'copy' : 'none';
        if (!drop || !target?.canAcceptDrop) return;
        const applicationFile = edit.readFileDrag?.(transfer);
        const files = applicationFile === undefined ? [...transfer.files] : [applicationFile];
        const directories = new Set();
        let index = 0;
        for (const item of transfer.items) {
          if (item.kind !== 'file') continue;
          const file = files[index++];
          if (file && item.webkitGetAsEntry?.()?.isDirectory) directories.add(file);
        }
        target.onAddFiles(files, directories);
      }
      const canUpload = typeof conversation?.createDrafts === 'function' && typeof shell.pickFiles === 'function';
      const uploadAllowed = props.useAccess('fileUploads/upload') !== false;
      return h('div', {
        className: 'dsh-revision-editor',
        onDragEnterCapture: event => targetFileDrag(event),
        onDragOverCapture: event => targetFileDrag(event),
        onDropCapture: event => targetFileDrag(event, true),
        onKeyDown: event => {
          if (event.key !== 'Escape' || event.defaultPrevented || event.nativeEvent.isComposing
            || !event.currentTarget.contains(event.target)
            || !event.target.closest('[contenteditable], .dsh-revision-cancel, .dsh-revision-text-part textarea')) return;
          event.preventDefault();
          event.stopPropagation();
          if (!pending) dismiss();
        },
      }, edit.additionalText.map((value, index) => h('label', {
        key: index, className: 'dsh-revision-text-part',
      }, `${edit.labels.textPart} ${index + 2}`, h('textarea', {
        defaultValue: value, disabled: pending || props.disabled,
        onChange: event => {
          edit.additionalText[index] = event.currentTarget.value;
          setPartsVersion(version => version + 1);
        },
      }))), h(edit.native.component, {
        ...props,
        t: (key, ...args) => t(key === 'input.commands' ? 'input.file' : key, ...args),
        variant: 'composer', occurrence: 'embedded',
        renderSlot: renderEditorSlot, useInput: useDraft, inputActions: shell.actions,
        keyboard: shell, useNotices, useMenuLauncher: closedMenu, useLexicon,
        useBusyEnter, useStopShortcut, useFileUploads: useCombinedUploads,
        registerFileIntake, resolveDraftAttachments, removeAttachment,
        addFiles: canUpload ? addFiles : undefined,
        retryFileUpload: id => { if (uploaded.has(id)) conversation.retryFileUpload(props.sessionId, id); },
        toggleCommandMenu: canUpload && uploadAllowed ? () => shell.pickFiles() : undefined,
        retry: undefined, stop: undefined,
        accessory: h('button', { type: 'button', className: 'dsh-revision-cancel', disabled: pending,
          title: cancelLabel, 'aria-label': cancelLabel, onClick: () => dismiss() }, h(TablerIcon, { name: 'x' })),
      }), sendRemainingText && h('button', { type: 'button', disabled: pending || props.disabled,
        onClick: () => void edit.submit(draftText, draftAttachments),
      }, edit.labels.sendPart));
    }

    return {
      inject: ['slots', 'locale'],
      apply(ctx) {
        const lifetime = new AbortController();
        let activeEditor;
        let sessionViews;
        let submitting = false;
        const savedDrafts = new Map();
        const draftListeners = new Set();
        let draftVersion = 0;
        const readDraftVersion = () => draftVersion;
        const observeDrafts = listener => {
          draftListeners.add(listener);
          return () => draftListeners.delete(listener);
        };
        function keepDraft(key, saved) {
          savedDrafts.set(key, saved);
          draftVersion++;
          for (const listener of draftListeners) listener();
        }
        let currentNotice = null;
        let noticeSequence = 0;
        const noticeListeners = new Set();
        const observeNotices = listener => {
          noticeListeners.add(listener);
          return () => noticeListeners.delete(listener);
        };
        const readNotice = () => currentNotice;
        function publishNotice(notice) {
          currentNotice = notice;
          for (const listener of noticeListeners) listener();
        }
        function notify(text, tone = 'warning') {
          publishNotice({ text, tone, seq: ++noticeSequence });
        }
        function Feedback({ renderFactorySlot }) {
          const notice = useSyncExternalStore(observeNotices, readNotice);
          if (!notice) return null;
          const onDone = () => { if (currentNotice === notice) publishNotice(null); };
          return renderFactorySlot(toastFactory, { ...notice, onDone }, {
            fallback: h(FallbackNotice, { key: notice.seq, notice, onDone }),
          });
        }
        function FallbackNotice({ notice, onDone }) {
          useEffect(() => {
            const timer = setTimeout(onDone, 2400);
            return () => clearTimeout(timer);
          }, [onDone]);
          return h('div', { role: notice.tone === 'success' ? 'status' : 'alert', className: 'dsh-revision-feedback' }, notice.text);
        }
        let composerFactoryLive = false;

        async function requestRevision(payload) {
          const response = await fetch('/api/message-revision', {
            method: 'POST', credentials: 'same-origin', signal: lifetime.signal,
            headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload),
          });
          const result = await response.json().catch(() => null);
          if (response.status === 401 || response.status === 403) throw new Error('authentication-required');
          if (!response.ok || result?.ok !== true) {
            throw Object.assign(new Error('failed'), {
              hostError: typeof result?.error === 'string' ? result.error : '',
              reconnect: result?.reconnect === true,
            });
          }
          return result;
        }

        function RevisionAction({ t, matched, sessionId, useChat, useSession, useAccess, renderFactorySlot }) {
          const allowed = useAccess('session/prompt');
          const status = useSession(s => s.removed || s.openState !== 'open' ? 'unavailable'
            : s.running || s.pendingSubmissions.length > 0 ? 'busy' : 'idle');
          const busy = status === 'busy';
          const nodes = useChat(s => s.nodes);
          const savedVersion = useSyncExternalStore(observeDrafts, readDraftVersion);
          const [edit, setEdit] = useState(null);
          const [opening, setOpening] = useState(false);
          const [tooltipDismissed, setTooltipDismissed] = useState(false);
          const pencil = useRef(null);
          const restoreFocus = useRef(false);
          const pending = useRef(false);
          const owner = useRef(Symbol());
          const draft = useRef(null);
          useEffect(() => {
            setEdit(null);
            setOpening(false);
            return () => {
              if (activeEditor === owner.current) activeEditor = undefined;
              draft.current?.dispose();
              draft.current = null;
            };
          }, [sessionId, matched]);
          useLayoutEffect(() => {
            if (!edit && restoreFocus.current) {
              restoreFocus.current = false;
              pencil.current?.focus({ preventScroll: true });
            }
          }, [edit]);
          useEffect(() => {
            if (!allowed || status !== 'idle' || edit || opening || activeEditor !== undefined || submitting) return;
            const node = messageInRow(pencil.current);
            if (!node) return;
            const saved = savedDrafts.get(revisionDraftKey(sessionId, node.data.messageId));
            if (saved?.reopenInView !== window.location.href) return;
            saved.reopenInView = undefined;
            void openEditor(node, saved);
          }, [allowed, status, edit, opening, nodes, matched, sessionId, savedVersion]);
          if (!allowed) return null;

          function errorMessage(error) {
            const message = typeof error?.hostError === 'string' ? error.hostError.trim() : '';
            const code = Object.hasOwn(strings.zh, message) ? message
              : Object.hasOwn(strings.zh, error?.message) ? error.message : null;
            return code ? t(code) : (message.slice(0, 240) || t('failed'));
          }

          function messageInRow(target) {
            const key = target?.closest('[data-chat-flow-kind="user"][data-chat-node-key]')?.dataset.chatNodeKey;
            const node = key === undefined ? undefined : nodes.get(key);
            return node?.kind === 'user' && node.data.source === matched && typeof node.data.messageId === 'string'
              ? node : undefined;
          }

          function open(event) {
            if (status !== 'idle') { notify(t(busy ? 'busy' : 'reconnect-unavailable')); return; }
            if (activeEditor !== undefined || submitting) {
              if (!opening) notify(t('anotherEdit'));
              return;
            }
            const node = messageInRow(event.currentTarget);
            if (!node) { notify(t('missing')); return; }
            const saved = savedDrafts.get(revisionDraftKey(sessionId, node.data.messageId));
            void openEditor(node, saved);
          }

          async function openEditor(node, saved) {
            const content = node.data.content;
            if (!Array.isArray(content)) { notify(t('unsupported')); return; }
            const native = nativeComposerEntry(ctx.slots);
            if (!native || !composerFactoryLive) { notify(t('composer-unavailable')); return; }
            const claim = owner.current = Symbol();
            activeEditor = claim;
            setOpening(true);
            try {
              const result = await requestRevision({ sessionId, messageId: node.data.messageId });
              if (activeEditor !== claim || lifetime.signal.aborted) return;
              if (typeof result.revision !== 'string' || typeof result.text !== 'string') throw new Error('failed');
              if (result.toolbarRevision !== 1 || !Array.isArray(result.attachments)
                || !Array.isArray(result.textParts) || result.textParts.some(part => typeof part !== 'string')
                || result.textParts.join('') !== result.text) throw new Error('toolbar-update');
              const conflicted = saved !== undefined && !canRestoreRevisionDraft(saved, result);
              const key = revisionDraftKey(sessionId, node.data.messageId);
              const initialParts = (conflicted ? undefined : saved?.textParts) ?? result.textParts ?? [result.text];
              const hasSegments = initialParts.length > 1;
              const firstText = conflicted ? saved.text : hasSegments ? initialParts[0] : result.text;
              const edit = {
                key, conflicted, restored: saved !== undefined,
                messageId: node.data.messageId, revision: result.revision, original: result.text, native,
                originalTextParts: result.textParts,
                additionalText: hasSegments && !conflicted ? initialParts.slice(1) : [],
                originalAttachments: JSON.stringify(result.attachments), settings: { ...saved?.settings },
                originalSettings: result.settings ?? {},
                modelDirectory: ctx.slots.entries('conversation.input.model')
                  .find(entry => entry.locale === 'model')?.inject?.(sessionId)?.directory,
                readFileDrag: transfer => ctx.get('fileDrag')?.read(transfer),
                labels: { fileDirectory: t('fileDirectory'), fileUnavailable: t('fileUnavailable'), textPart: t('textPart'), sendPart: t('sendPart') },
                submit: (text, attachmentIds) => submit(edit, text, attachmentIds, claim),
              };
              edit.composer = openDraft(ctx, native, sessionId, firstText, result.attachments,
                edit.submit,
                conflicted ? { ...saved, retainedIndices: [] } : hasSegments && saved ? { ...saved, text: firstText } : saved);
              if (conflicted) notify(t('draftConflict'));
              if (savedDrafts.get(key) === saved) savedDrafts.delete(key);
              draft.current = edit.composer;
              setOpening(false);
              setEdit(edit);
            } catch (error) {
              if (activeEditor !== claim || lifetime.signal.aborted) return;
              activeEditor = undefined;
              setOpening(false);
              notify(errorMessage(error));
            }
          }

          function dismiss(restore = true) {
            if (activeEditor === owner.current) activeEditor = undefined;
            restoreFocus.current = restore === true;
            const composer = draft.current;
            draft.current = null;
            setEdit(null);
            composer?.dispose();
          }

          async function submit(edit, text, attachmentIds, claim) {
            if (edit.conflicted) {
              notify(t('draftConflict'));
              return { kind: 'error' };
            }
            const textParts = edit.additionalText.length ? [text, ...edit.additionalText] : undefined;
            if (textParts) text = textParts.join('');
            if (submitting || pending.current || activeEditor !== claim || (!text.trim() && attachmentIds.length === 0)) {
              return { kind: 'error' };
            }
            const { retained, conversation } = edit.composer;
            const remaining = new Set(attachmentIds);
            const removedAttachments = [...retained].filter(([id]) => !remaining.has(id)).map(([, item]) => item.index);
            const uploads = conversation?.fileUploads.getSnapshot() ?? emptyUploads;
            const receiptIds = [];
            for (const id of attachmentIds) {
              if (retained.has(id)) continue;
              const upload = uploads[id];
              if (upload?.status !== 'ready' || typeof upload.receiptId !== 'string') {
                notify(t('uploadWait'));
                return { kind: 'error' };
              }
              receiptIds.push(upload.receiptId);
            }
            const selectedModel = edit.settings.model;
            const originalModel = edit.originalSettings.model;
            const modelGroup = edit.modelDirectory?.getSnapshot()?.groups?.find(group => group.id === originalModel?.provider);
            const defaultEffort = modelGroup?.models.find(model => model.id === originalModel?.model)?.reasoning?.defaultEffort;
            const sameModel = selectedModel === undefined || (selectedModel.provider === originalModel?.provider
              && selectedModel.model === originalModel?.model
              && (selectedModel.reasoningEffort ?? defaultEffort) === (originalModel?.reasoningEffort ?? defaultEffort));
            const samePermission = edit.settings.permissionPreset === undefined
              || edit.settings.permissionPreset === edit.originalSettings.permissionPreset;
            const sameText = text === edit.original
              && (textParts === undefined || textParts.every((part, index) => part === edit.originalTextParts[index]));
            if (sameText && removedAttachments.length === 0 && receiptIds.length === 0
              && sameModel && samePermission) {
              dismiss();
              return { kind: 'success' };
            }
            if (text.length > 200_000) {
              notify(t('tooLong'));
              return { kind: 'error' };
            }
            let saved;
            try {
              saved = snapshotRevisionDraft(edit, text, attachmentIds, textParts);
            } catch (error) {
              notify(errorMessage(error));
              return { kind: 'error' };
            }
            const view = window.location.href;
            submitting = true;
            pending.current = true;
            let reconnect;
            let restoreDraft = false;
            let savedForRetry = false;
            const inView = () => !lifetime.signal.aborted && window.location.href === view;
            try {
              if (!sessionViews) throw new Error('reconnect-unavailable');
              reconnect = sessionViews.holdSessionView(sessionId);
              let result;
              try {
                result = await requestRevision({ sessionId, messageId: edit.messageId,
                  toolbarRevision: 1, text, textParts, expectedRevision: edit.revision,
                   removedAttachments, receiptIds, ...edit.settings });
              } catch (error) {
                if (error.reconnect || reconnect.removed) {
                  restoreDraft = true;
                  await reconnect.reconnect().catch(() => {});
                }
                throw error;
              }
              try {
                if (result.unchanged === true) {
                  if (inView() && activeEditor === claim) dismiss();
                  return { kind: 'success' };
                }
                if (result.reload === true || reconnect.removed) await reconnect.reconnect();
                if (inView()) {
                  if (activeEditor === claim) dismiss();
                  if (result.cleanupWarning) notify(t('cleanupPending'));
                  else notify(t('done'), 'success');
                }
              } catch {
                if (inView()) {
                  if (activeEditor === claim) dismiss();
                  notify(t('syncing'));
                }
              }
              return { kind: 'success' };
            } catch (error) {
              savedForRetry = !lifetime.signal.aborted && (restoreDraft || draft.current !== edit.composer);
              if (savedForRetry && activeEditor === claim) dismiss(false);
              if (inView()) notify(savedForRetry ? `${errorMessage(error)} ${t('draftSaved')}` : errorMessage(error));
              return { kind: 'error' };
            } finally {
              pending.current = false;
              try {
                reconnect?.release();
              } finally {
                submitting = false;
                if (savedForRetry) keepDraft(edit.key, { ...saved, reopenInView: inView() ? view : undefined });
              }
            }
          }

          return h(Fragment, null,
            !edit && h('button', {
              ref: pencil, type: 'button', className: 'dsh-revision-pencil', onClick: open,
              'aria-label': t('edit'), 'aria-busy': opening || undefined,
              'data-tooltip': t('edit'), 'data-tooltip-dismissed': (opening || tooltipDismissed) || undefined,
              onFocus: () => setTooltipDismissed(false),
              onPointerLeave: () => setTooltipDismissed(false),
              onKeyDown: event => { if (event.key === 'Escape') setTooltipDismissed(true); },
            }, h(TablerIcon, { name: 'pencil' })),
            edit && renderFactorySlot(editorFactory, {
              edit, dismiss, cancelLabel: t('cancel'), disabled: busy,
            }));
        }

        ctx.effect(() => () => lifetime.abort());
        ctx.inject(['sessions'], scoped => scoped.effect(() => {
          const views = watchSessionViews(sessionRuntime(scoped));
          sessionViews = views;
          return () => {
            if (sessionViews === views) sessionViews = undefined;
            views?.dispose();
          };
        }));
        ctx.effect(() => ctx.locale.register(locale, strings));
        // The same native child seats remain available to the registered native bar.
        const composerChildren = {
          'conversation.input.attachments': { kind: 'single', scope: 'session-maybe' },
          'conversation.input.permission': { kind: 'single', scope: 'session' },
          'conversation.input.plan': { kind: 'single', scope: 'session' },
          'conversation.input.model': { kind: 'single', scope: 'session' },
          'conversation.input.activity': { kind: 'single', scope: 'session' },
          'conversation.input.left': { kind: 'list', scope: 'session' },
          'conversation.input.right': { kind: 'list', scope: 'session' },
          'conversation.composer.dock': { kind: 'list', scope: 'session' },
        };
        ctx.slots.inject(composerSlot, () => {
          // rc.5 registers and tears down only own child declarations, but the
          // Factory renderer resolves their specs by lookup. Borrow the already
          // live slots without owning them so unloading leaves the host intact.
          const borrowedChildren = Object.freeze(Object.create(Object.freeze(composerChildren)));
          const dispose = ctx.slots.registerFactory({
            name: editorFactory, scope: 'session', locale: 'conversation', children: borrowedChildren,
          }, NativeEditor);
          composerFactoryLive = true;
          return () => { composerFactoryLive = false; dispose(); };
        });
        ctx.slots.inject('shell.overlay', () => ctx.slots.register({
          name: 'shell.overlay', id: 'message-revision.styles', locale,
        }, props => h(Fragment, null, h('style', null, css), h(Feedback, props))));
        ctx.slots.inject('conversation.chat.user.before', () => ctx.slots.register({
          name: 'conversation.chat.user.before', locale, order: 10000,
          select: ({ kind, source }) => kind === 'user' && source?.kind === 'user' ? source : null,
        }, RevisionAction));
      },
    };
  },
});
