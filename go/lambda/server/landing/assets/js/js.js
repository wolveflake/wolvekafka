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

    window.addEventListener('DOMContentLoaded', function () {
        document.querySelectorAll('.grafana-frame').forEach(function (frame) {
            setupCard(frame);
        });
    });
})();
