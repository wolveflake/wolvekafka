(function () {
    const storagePrefix = 'wolvekafka.grafana.';

    function persist(key, value) {
        try {
            window.localStorage.setItem(storagePrefix + key, value || '');
        } catch (err) {
            console.warn('localStorage unavailable, embeds will reset on refresh', err);
        }
    }

    function read(key) {
        try {
            return window.localStorage.getItem(storagePrefix + key);
        } catch (err) {
            console.warn('localStorage unavailable', err);
            return null;
        }
    }

    function loadFrame(frame, url, storageKey) {
        if (!frame || !url) {
            return;
        }
        frame.src = url;
        persist(storageKey, url);
    }

    function setupCard(frame) {
        const frameId = frame.id;
        const storageKey = frameId;
        const defaultUrl = frame.dataset.default || frame.getAttribute('src') || '';
        const input = document.querySelector(`.grafana-input[data-frame-target="${frameId}"]`);
        const loadButton = document.querySelector(`.grafana-load[data-frame-target="${frameId}"]`);
        const resetButton = document.querySelector(`.grafana-reset[data-frame-target="${frameId}"]`);

        if (!input) {
            return;
        }

        const remembered = read(storageKey);
        if (remembered) {
            input.value = remembered;
            frame.src = remembered;
        } else if (!input.value && defaultUrl) {
            input.value = defaultUrl;
        }

        loadButton?.addEventListener('click', function () {
            const nextUrl = input.value.trim();
            if (!nextUrl) {
                input.focus();
                input.reportValidity?.();
                return;
            }
            loadFrame(frame, nextUrl, storageKey);
        });

        resetButton?.addEventListener('click', function () {
            input.value = defaultUrl;
            loadFrame(frame, defaultUrl, storageKey);
        });
    }

    function setupDemoProducerForm() {
        const form = document.getElementById('demo-producer-form');
        if (!form) {
            return;
        }

        const statusEl = document.getElementById('demo-producer-status');
        const submitBtn = form.querySelector('[data-role="producer-submit"]');
        const resetBtn = form.querySelector('[data-role="producer-reset"]');
        const defaults = {
            topic: form.topic.value,
            bootstrap: form.bootstrap_servers.value,
            count: form.count.value,
            interval: form.interval_seconds.value,
            message: form.message.value,
        };

        function setStatus(kind, message) {
            if (!statusEl) {
                return;
            }
            statusEl.classList.add('d-none');
            statusEl.classList.remove('alert-success', 'alert-danger', 'alert-info');
            if (!message) {
                return;
            }
            statusEl.classList.remove('d-none');
            statusEl.classList.add('alert-' + kind);
            statusEl.textContent = message;
        }

        form.addEventListener('submit', async function (event) {
            event.preventDefault();
            if (!form.reportValidity()) {
                return;
            }

            const payload = {
                topic: form.topic.value.trim(),
                bootstrap_servers: form.bootstrap_servers.value.trim(),
                count: Number(form.count.value) || 0,
                interval_seconds: Number(form.interval_seconds.value) || 0,
                message: form.message.value.trim(),
            };

            setStatus('info', 'Starting producer...');
            submitBtn.disabled = true;
            try {
                const response = await fetch('/api/demo-producer', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload),
                });
                if (!response.ok) {
                    const text = await response.text();
                    throw new Error(text || 'Producer call failed');
                }
                const result = await response.json();
                const last = (result.results || []).slice(-1)[0];
                const summary = last
                    ? `Produced ${result.produced} message(s). Last offset ${last.offset}.`
                    : `Produced ${result.produced} message(s).`;
                setStatus('success', summary);
            } catch (err) {
                console.error('producer failed', err);
                setStatus('danger', err.message || 'Failed to run producer');
            } finally {
                submitBtn.disabled = false;
            }
        });

        resetBtn?.addEventListener('click', function () {
            form.topic.value = defaults.topic;
            form.bootstrap_servers.value = defaults.bootstrap;
            form.count.value = defaults.count;
            form.interval_seconds.value = defaults.interval;
            form.message.value = defaults.message;
            setStatus('', '');
        });
    }

    window.addEventListener('DOMContentLoaded', function () {
        document.querySelectorAll('.grafana-frame').forEach(function (frame) {
            setupCard(frame);
        });
        setupDemoProducerForm();
    });
})();
