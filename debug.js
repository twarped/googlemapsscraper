document.addEventListener('click', (e) => {
    if (!document.getElementById('flash-point-style')) {
        const styleEl = document.createElement('style');
        styleEl.id = 'flash-point-style';
        styleEl.textContent = `
            @keyframes show-pointer-ani {
                0% { opacity: 1; transform: scale(1, 1); }
                50% { transform: scale(3, 3); }
                100% { transform: scale(1, 1); opacity: 0; }
            }
        `;
        document.head.appendChild(styleEl);
    }

    const flash = document.createElement('div');
    const size = 10;
    const duration = 1;
    flash.style.cssText = `
        position:absolute;
        z-index:2147483647;
        left:${e.pageX}px;
        top:${e.pageY}px;
        width:${size}px;
        height:${size}px;
        border-radius:50%;
        background:red;
        translate:-50% -50%;
        opacity: 0;
        animation:show-pointer-ani ${duration}s ease 1;
    `;
    document.body.appendChild(flash);

    setTimeout(() => flash.remove(), duration * 1000);
});

document.addEventListener('mousemove', (e) => {
    if (!document.getElementById('flash-point-style')) {
        const styleEl = document.createElement('style');
        styleEl.id = 'flash-point-style';
        styleEl.textContent = `
            @keyframes show-pointer-ani {
                0% { opacity: 1; transform: scale(1, 1); }
                50% { transform: scale(3, 3); }
                100% { transform: scale(1, 1); opacity: 0; }
            }
        `;
        document.head.appendChild(styleEl);
    }

    const flash = document.createElement('div');
    const size = 5;
    const duration = 1;
    flash.style.cssText = `
        position:absolute;
        z-index:2147483647;
        left:${e.pageX}px;
        top:${e.pageY}px;
        width:${size}px;
        height:${size}px;
        border-radius:50%;
        background:cyan;
        translate:-50% -50%;
        opacity:0;
        pointer-events:none;
        will-change: transform, opacity;
        animation:show-pointer-ani ${duration}s ease 1;
    `;
    document.body.appendChild(flash);

    setTimeout(() => flash.remove(), duration * 1000);
});

document.addEventListener('wheel', (e) => {
    if (!document.getElementById('flash-point-style')) {
        const styleEl = document.createElement('style');
        styleEl.id = 'flash-point-style';
        styleEl.textContent = `
            @keyframes show-pointer-ani {
                0% { opacity: 1; transform: scale(1, 1); }
                50% { transform: scale(3, 3); }
                100% { transform: scale(1, 1); opacity: 0; }
            }
        `;
        document.head.appendChild(styleEl);
    }

    const flash = document.createElement('div');
    const size = 8;
    const duration = 1;
    flash.style.cssText = `
        position:absolute;
        z-index:2147483647;
        left:${e.pageX}px;
        top:${e.pageY}px;
        width:${size}px;
        height:${size}px;
        border-radius:50%;
        background:lime;
        translate:-50% -50%;
        opacity:0;
        pointer-events:none;
        will-change: transform, opacity;
        animation:show-pointer-ani ${duration}s ease 1;
    `;
    document.body.appendChild(flash);

    setTimeout(() => flash.remove(), duration * 1000);
});
